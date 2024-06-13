package com.nebula.hs.integration

import com.hs.common.SparkCommon
import com.hs.config.DeltaToJdbcConfigV2
import com.hs.utils.CommonFunction.sendToKafka
import com.hs.utils.{JdbcWriter, OffsetUtil, Supplement, Transformer}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import java.text.SimpleDateFormat
import java.util.Date


object DeltaToJdbc {
  val offsetPath = "/delta/_offset/delta2jdbc/%s"
  val jobType = "Delta2Jdbc"
  var messageBuf = new StringBuilder("{")

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常!")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val deltaToJdbcConfig = JsonMethods.parse(params).extract[DeltaToJdbcConfigV2]

    /**
     * spark session 配置信息
     */
    val sparkConfig = SparkCommon.sparkConfig
    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${deltaToJdbcConfig.taskID}")

    /**
     * Delta SparkSession
     */
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)

    val jdbcWriteOptions = Map(
      "url" -> deltaToJdbcConfig.jdbcUrl,
      "user" -> deltaToJdbcConfig.username,
      "password" -> deltaToJdbcConfig.password,
      "batchsize" -> deltaToJdbcConfig.batchSize,
      "dbtable" -> s"${deltaToJdbcConfig.targetDatabase}.${deltaToJdbcConfig.targetTable}"
    )

    //获取delta df
    var df = spark.read.format("delta").load(deltaToJdbcConfig.tablePathFormat.format(deltaToJdbcConfig.sourceDatabase, deltaToJdbcConfig.sourceTable))

    //查询过滤条件
    val sql = OffsetUtil.getDeltaQuerySql(spark, deltaToJdbcConfig)
    println("querySql=" + sql)
    if (sql != null && sql.nonEmpty) {
      df.createOrReplaceTempView("tmp_" + deltaToJdbcConfig.taskID)
      df = spark.sql(sql)
    }

    /**
     * 增量读取时，获取数据中offset最大值
     */
    var maxOffsetDf: DataFrame = null
    var offsetColumns = deltaToJdbcConfig.offsetColumns
    if ("incr".equals(deltaToJdbcConfig.readMode)) {
      maxOffsetDf = df.selectExpr(s"max(${deltaToJdbcConfig.offsetColumns}) as offset ").withColumn("taskID", lit(deltaToJdbcConfig.taskID))

      //获取source和target的字段映射关系
      //var columnMapFinal = Map[String, String]()
      //deltaToJdbcConfig.columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
      //offsetColumns = columnMapFinal(deltaToJdbcConfig.offsetColumns)
    }

    df.createOrReplaceTempView("tmpTable_" + deltaToJdbcConfig.taskID)

    var finalDF = spark.sql(Transformer.fieldNameTransform(deltaToJdbcConfig.columnMap, deltaToJdbcConfig.taskID))
    if (deltaToJdbcConfig.isWatermark.equals("1")) {
      finalDF = Supplement.traceData(deltaToJdbcConfig.taskID, finalDF)
    }
    //    finalDF.show(false)
    //    finalDF.printSchema()
    val writer = new JdbcWriter(finalDF, deltaToJdbcConfig.writeMode, jdbcWriteOptions)
    writer.write(deltaToJdbcConfig.mergeKeys)

    //更新offset
    if ("incr".equals(deltaToJdbcConfig.readMode)) {
      maxOffsetDf.write.option("header", value = true).mode(SaveMode.Overwrite).csv(offsetPath.format(deltaToJdbcConfig.taskID))
    }

    val syncTime: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    // 统计信息发送kafka
    if (deltaToJdbcConfig.kafkaServers != null && deltaToJdbcConfig.topic != null) {
      messageBuf.append(
        "\"id\":\"%s\"".format(deltaToJdbcConfig.taskID)
          + ","
          + "\"dataNum\":%d".format(finalDF.count())
          + ","
          + "\"apiDataTypeEnum\":\"%s\"".format("CHANNEL_TYPE_OFFLINE_3")
          + ","
          + "\"syncTime\":\"%s\"".format(syncTime)
          + "}"
      )
      sendToKafka(messageBuf.toString, deltaToJdbcConfig.kafkaServers, deltaToJdbcConfig.topic)
    }
  }
}
