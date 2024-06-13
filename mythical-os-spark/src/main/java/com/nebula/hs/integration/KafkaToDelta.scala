package com.nebula.hs.integration

/*
 *@Author   : DoubleTrey
 *@Time     : 2022/10/18 15:14
 */


import com.hs.common.{DataType, SparkCommon}
import com.hs.config.KafkaToDeltaJobConfig
import com.hs.utils.DeltaMergeUtil.orderExpr
import com.hs.utils.{StreamWriter, Supplement, Transformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import java.util.concurrent.TimeUnit

object KafkaToDelta {
  val jobType = "Kafka2Delta"
  val checkpointPathFormat = "/delta/events/_checkpoints/kafka2stg/%s"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[KafkaToDeltaJobConfig]
    //println(jobConf)

    val checkpointPath = checkpointPathFormat.format(jobConf.taskID)

    /**
     * spark session 配置信息
     */
    val sparkConfig = SparkCommon.sparkConfig
    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${jobConf.taskID}")
    sparkConfig.setIfMissing("spark.sql.streaming.checkpointLocation", checkpointPath) // checkpoint

    /**
     * Delta SparkSession
     */
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)

    /**
     * 日志级别
     */
    spark.sparkContext.setLogLevel("WARN")

    /**
     * readStream 配置信息
     */
    val readStreamOptions = Map(
      "kafka.bootstrap.servers" -> jobConf.kafkaServers,
      "spark.sql.streaming.forceDeleteTempCheckpointLocation" -> "true",
      "minOffsetsPerTrigger" -> "1",
      "maxOffsetsPerTrigger" -> jobConf.batchSize,
      "maxTriggerDelay" -> jobConf.consumeDelay,
      "startingOffsets" -> jobConf.consumeMode,
      "subscribe" -> jobConf.topicName,
      "failOnDataLoss" -> jobConf.failOnDataLoss
    )

    /**
     * writeStream 配置信息
     */
    val writeStreamOptions = Map(
      "checkpointLocation" -> checkpointPath
    )

    /**
     * 根据入参 "源字段类型" 生成 schema
     */
    val schema = StructType(
      jobConf.sourceColumns.split(",").map(x => StructField(x.split(":")(0), DataType.getType(x.split(":")(1)), nullable = true))
    )

    /**
     * 消费kafka，根据源数据 schema 生成 DataFrame
     *
     * TODO: 源字段与目标字段不一致，重命名（ps: 分层设计上要求stg层与原始内容一致，不需要重命名）
     */
    var df = spark.readStream.format("kafka")
      .options(readStreamOptions)
      .load()

    /**
     * kafka connector 数据内容处理 FIXME: CDC op = ''d'', 上游数据删除操作时, stg 表逻辑删除字段 is_delete 支持
     *
     * @param df    [[DataFrame]]
     * @param isCDC [[String]]
     * @return
     */
    def kafkaConnectorDataTransform(df: DataFrame, isCDC: String): DataFrame = {
      val resultDF = df.selectExpr("CAST(value AS String)", "timestamp as insert_time")
      if (isCDC == "true") {
        /**
         * CDC 数据
         */
        resultDF.withColumn("before", get_json_object(col("value"), "$.payload.before"))
          .withColumn("after", get_json_object(col("value"), "$.payload.after"))
          .withColumn("is_delete", when(get_json_object(col("value"), "$.payload.op").equalTo("d"), 1).otherwise(0))
          .selectExpr("CASE `is_delete` WHEN 1 THEN `before` ELSE `after` END AS data", "is_delete", "insert_time")
          .withColumn("data", from_json(col("data"), schema))
          .selectExpr("data.*", "is_delete", "insert_time")
      } else {
        /**
         * 非 CDC 数据
         */
        resultDF.withColumn("data", from_json(get_json_object(col("value"), "$.payload"), schema))
          .selectExpr("data.*", "insert_time")
      }
    }

    if (jobConf.isKafkaConnector == "true") {
      df = kafkaConnectorDataTransform(df, jobConf.isCDC)
    } else {
      /**
       * 读取自定义 kafka 数据, 目前只支持 value 为 json 且解析 json 一级字段
       */
      df = df.withColumn("data", from_json(col("value").cast("string"), schema))
        .selectExpr("data.*", "timestamp as insert_time")
    }
    // 打印数据结构
    df.printSchema()

    df.createOrReplaceTempView("tmpTable_" + jobConf.taskID)
    var finalDF = spark.sql(Transformer.fieldNameTransformKafka(jobConf.columnMap, jobConf.taskID))

    /**
     * 系统字段填充
     */
    finalDF = Supplement.systemFieldComplete(finalDF)

    /**
     * 分区字段处理
     */
    finalDF = Supplement.partitionTrans(finalDF, jobConf.zoneType, jobConf.zoneFieldCode, jobConf.zoneTargetFieldCode, jobConf.zoneTypeUnit)

    /**
     * 敏感字段加密处理
     */
    finalDF = Supplement.encryptTrans(finalDF, jobConf.encrypt)

    val accumulator: LongAccumulator = spark.sparkContext.longAccumulator("dataCount")

    val writer = new StreamWriter(finalDF, jobConf.writeMode.toLowerCase, Option(writeStreamOptions), Option(1), Option(TimeUnit.MINUTES))
    writer.write(jobConf.tablePathFormat.format(jobConf.targetDatabase, jobConf.targetTable), Option(jobConf.mergeKeys), Option(orderExpr("insert_time:-1")))

    /**
     * 接入数据量统计
     */
    finalDF.writeStream
      .foreachBatch(
        (batchDF: Dataset[Row], batchId: Long) => {
          println("batchID:" + batchId)
          accumulator.add(batchDF.count())
          println(accumulator.value)
          Supplement.dataCount(jobConf.taskID, "kafka", accumulator.value, jobConf.dataCountKafkaServers, jobConf.dataCountTopicName, jobConf.targetDatabase)
        })
      .trigger(Trigger.ProcessingTime(0))
      .start
    spark.streams.awaitAnyTermination()
  }
}
