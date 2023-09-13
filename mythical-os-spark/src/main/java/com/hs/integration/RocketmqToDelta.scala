package com.hs.integration

import com.hs.common.{DataType, SparkCommon}
import com.hs.config.RocketmqToDeltaJobConfig
import com.hs.utils.DeltaMergeUtil.orderExpr
import com.hs.utils.{StreamWriter, Supplement, Transformer}
//import org.apache.rocketmq.common.topic.TopicValidator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import java.util.concurrent.TimeUnit


object RocketmqToDelta {
  val jobType = "Rocketmq2Delta"
  val checkpointPathFormat = "/delta/events/_checkpoints/Rocketmq2stg/%s"
  //  val checkpointPathFormat = "./%s"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[RocketmqToDeltaJobConfig]

    //rocketmq group 校验
    //    if (StringUtils.isNotBlank(jobConf.group) && TopicValidator.isTopicOrGroupIllegal(jobConf.group)) {
    //      throw new IllegalArgumentException(String.format(
    //        "Rocketmq group参数不合法，the specified group[%s] contains illegal characters, allowing only %s", jobConf.group,
    //        "^[%|a-zA-Z0-9_-]+$"), null)
    //    }

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
      "startingOffsets" -> jobConf.consumeMode,
      "failOnDataLoss" -> jobConf.failOnDataLoss,
      "nameServer" -> jobConf.nameServer,
      "topic" -> jobConf.topic,
      "group" -> jobConf.group,
      "pullBatchSize" -> jobConf.pullBatchSize,
      "pullTimeout" -> jobConf.pullTimeout,
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
    var df = spark.readStream.format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
      .options(readStreamOptions)
      .load()


    /**
     * 读取自定义 Rocketmq 数据,jobConf为空解析json一级字段，jobConf为不为空解析json多级字段
     * Rocketmq消息样例：{"a":{"b":{"id":"23","name":"huoshi"}}}   jsonPath样例："a.b"
     * Rocketmq消息样例：{"id":"2","name":"huoshi"}   jsonPath样例：""
     */
    if (jobConf.jsonPath != null && jobConf.jsonPath.nonEmpty) {
      val jsonPath = "$." + jobConf.jsonPath
      df = df.selectExpr("CAST(body AS " + jobConf.valueType + ")", "bornTimestamp")
        .withColumn("msg", from_json(get_json_object(col("body"), jsonPath), schema))
        .selectExpr("msg.*")
    } else {
      df = df.selectExpr("CAST(body AS " + jobConf.valueType + ")", "bornTimestamp")
        .withColumn("msg", from_json(col("body"), schema))
        .selectExpr("msg.*")
    }

    // 打印数据结构
    df.printSchema()

    df.createOrReplaceTempView("tmpTable_" + jobConf.taskID)
    var finalDF = spark.sql(Transformer.fieldNameTransform(jobConf.columnMap, jobConf.taskID))

    /**
     * 系统字段填充
     */
    finalDF = Supplement.systemFieldComplete(finalDF)

    //本地调试，输出控制台
    //    finalDF.createOrReplaceTempView("test")
    //    spark.sql("select *  from test").writeStream
    //      .outputMode("append")
    //      .format("console")
    //      .start()

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
          Supplement.dataCount(jobConf.taskID, "rocketmq", accumulator.value, jobConf.dataCountKafkaServers, jobConf.dataCountTopicName, jobConf.targetDatabase)
        })
      .trigger(Trigger.ProcessingTime(0))
      .start
    spark.streams.awaitAnyTermination()
  }
}
