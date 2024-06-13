package com.nebula.hs.integration

import com.hs.common.SparkCommon
import com.hs.config.JdbcToDeltaJobConfig
import com.hs.utils.DeltaMergeUtil.sortConfig
import com.hs.utils._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object HiveToDelta {
  val jobType = "Hive2Delta"
  val offsetPath = "/delta/_offset/jdbc2stg/%s"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[JdbcToDeltaJobConfig]

    /**
     * spark session 配置信息
     */
    val sparkConfig = SparkCommon.sparkConfig
    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${jobConf.taskID}")

    /**
     * Delta SparkSession
     */
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)

    /**
     * Register hive jdbc dialect
     */
    JdbcDialects.registerDialect(new HiveJdbcDialet)
    /**
     * 读取hdfs上的offset,拼接查询sql
     */
    val querySql = OffsetUtil.getQuerySql(spark, jobConf)
    println("querySql为：" + querySql)


    var hiveJdbcUrl = jobConf.jdbcUrl
    val hiveUniqueColumnConfig = "hive.resultset.use.unique.column.names=false"
    hiveJdbcUrl = if (hiveJdbcUrl.contains("?")) {
      hiveJdbcUrl + "&" + hiveUniqueColumnConfig
    } else {
      hiveJdbcUrl + "?" + hiveUniqueColumnConfig
    }

    /**
     * spark read jdbc options
     */
    val jdbcReaderOptions = Map(
      "url" -> hiveJdbcUrl,
      "user" -> jobConf.username,
      "password" -> jobConf.password,
      "query" -> querySql,
      "fetchsize" -> jobConf.fetchSize,
      "pushDownLimit" -> "true"
    )

    val sourceDF = spark.read
      .format("jdbc")
      .options(jdbcReaderOptions)
      .load()
    //      .withColumn("insert_time", current_timestamp())

    /**
     * 增量读取时，获取数据中offset最大值
     */
    var maxOffsetDf: DataFrame = null
    var offsetColumns = jobConf.offsetColumns
    if ("incr".equals(jobConf.readMode)) {
      maxOffsetDf = sourceDF.selectExpr(s"max(${jobConf.offsetColumns}) as offset ").withColumn("taskID", lit(jobConf.taskID))

      //获取source和target的字段映射关系
      var columnMapFinal = Map[String, String]()
      jobConf.columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
      offsetColumns = columnMapFinal(jobConf.offsetColumns)
    }
    //    sourceDF.show()
    //    sourceDF.printSchema()
    /**
     * 字段名字和类型映射转换
     */
    var finalDF = Transformer.hiveFieldNameAndTypeTransform(sourceDF, jobConf.sourceColumns, jobConf.columnMap)
    //.withColumn("insert_time", current_timestamp())

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

    val targetTablePath = jobConf.tablePathFormat.format(jobConf.targetDatabase, jobConf.targetTable)
    val writer = FrameWriter(finalDF, jobConf.writeMode.toLowerCase, Option(null))
    //writer.write(targetTablePath, Option(jobConf.mergeKeys), Option(orderExpr(s"${jobConf.offsetColumns.split(",")(0)}:-1")))
    writer.write(targetTablePath, Option(jobConf.mergeKeys), sortConfig(jobConf.writeMode, jobConf.offsetColumns))

    /**
     * 增量读取时，存储数据中offset最大值
     */
    if ("incr".equals(jobConf.readMode) && maxOffsetDf.filter(col("offset").isNotNull).count() > 0) {
      maxOffsetDf.write.option("header", value = true).mode(SaveMode.Overwrite).csv(offsetPath.format(jobConf.taskID))
    }

    /**
     * 接入数据量统计
     */
    Supplement.dataCount(jobConf.taskID, "hive", finalDF.count(), jobConf.dataCountKafkaServers, jobConf.dataCountTopicName, jobConf.targetDatabase)
  }
}
