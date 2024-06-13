/*
 *@Author   : DoubleTrey
 *@Time     : 2022/11/4 17:19
 */

package com.nebula.hs.integration

import com.hs.common.SparkCommon
import com.hs.config.JdbcToDeltaJobConfig
import com.hs.utils.DeltaMergeUtil._
import com.hs.utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object JdbcToDelta {
  val offsetPath = "/delta/_offset/jdbc2stg/%s"
  val jobType = "Jdbc2Delta"

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
     * 注册 SqlServer 自定义方言
     */
    JdbcDialects.registerDialect(new MsSqlServerJdbcDialect)

    /**
     * 注册 Oracle 自定义方言
     */
    JdbcDialects.registerDialect(new OracleJdbcDialect)

    /**
     * 注册 Kingbase 自定义方言
     */
    JdbcDialects.registerDialect(new KingbaseJdbcDialect)

    /**
     * 注册 Dm 自定义方言
     */
    JdbcDialects.registerDialect(new DmJdbcDialect)

    /**
     * 读取hdfs上的offset,拼接查询sql
     */
    val querySql = OffsetUtil.getQuerySql(spark, jobConf)
    println("querySql为：" + querySql)

    /**
     * spark read jdbc options
     */
    val customSchema = jobConf.sourceColumns.split(",").map(col => {
      "`" + col.split(":")(0) + "` " + col.split(":")(1).replace("DECIMAL", "DECIMAL(38,8)")
    }).mkString(",")

    val jdbcReaderOptions = Map(
      "url" -> jobConf.jdbcUrl,
      "user" -> jobConf.username,
      "password" -> jobConf.password,
      "query" -> querySql,
      "fetchsize" -> jobConf.fetchSize,
      "pushDownLimit" -> "true",
      "customSchema" -> customSchema,
      "queryTimeout" -> "3600"
    )

    val sourceDF = spark.read
      .format("jdbc")
      .options(jdbcReaderOptions)
      .load()
      .withColumn("insert_time", current_timestamp())

    /**
     * 增量读取时，获取数据中offset最大值
     */
    var maxOffsetDf: DataFrame = null
    var offsetColumns = jobConf.offsetColumns
    if ("incr".equals(jobConf.readMode)) {
      maxOffsetDf = sourceDF.selectExpr(s"cast(max(${jobConf.offsetColumns}) as string) as offset ").withColumn("taskID", lit(jobConf.taskID))

      //获取source和target的字段映射关系
      var columnMapFinal = Map[String, String]()
      jobConf.columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
      offsetColumns = columnMapFinal(jobConf.offsetColumns)
    }

    /**
     * 字段映射转换
     */
    sourceDF.createOrReplaceTempView("tmpTable_" + jobConf.taskID)
    var finalDF = spark.sql(Transformer.fieldNameTransform(jobConf.columnMap, jobConf.taskID))

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
    val writer = new FrameWriter(finalDF, jobConf.writeMode.toLowerCase, Option(null))
    //writer.write(targetTablePath, Option(jobConf.mergeKeys), Option(orderExpr(s"${jobConf.offsetColumns.split(",")(0)}:-1")))
    writer.write(targetTablePath, Option(jobConf.mergeKeys), sortConfig(jobConf.writeMode, offsetColumns))

    /**
     * 增量读取时，存储数据中offset最大值
     */
    if ("incr".equals(jobConf.readMode) && maxOffsetDf.filter(col("offset").isNotNull).count() > 0) {
      maxOffsetDf.write.option("header", value = true).mode(SaveMode.Overwrite).csv(offsetPath.format(jobConf.taskID))
    }

    /**
     * 接入数据量统计
     */
    Supplement.dataCount(jobConf.taskID, jobConf.jdbcUrl.split(":")(1), finalDF.count(), jobConf.dataCountKafkaServers, jobConf.dataCountTopicName, jobConf.targetDatabase)
  }
}
