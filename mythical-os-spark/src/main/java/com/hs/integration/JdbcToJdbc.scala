package com.hs.integration

import com.hs.common.SparkCommon
import com.hs.config.JdbcToJdbcJobConfig
import com.hs.utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object JdbcToJdbc {
  val offsetPath = "/delta/_offset/jdbc2stg/%s"
  val jobType = "Jdbc2Jdbc"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)

    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[JdbcToJdbcJobConfig]

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
     * 读取hdfs上的offset,拼接查询sql
     */
    val querySql = OffsetUtil.getJdbcQuerySql(spark, jobConf)
    println("querySql为：" + querySql)

    /**
     * spark read jdbc options
     */
    val customSchema = jobConf.sourceColumns.split(",").map(col => {
      "`" + col.split(":")(0) + "` " + col.split(":")(1).replace("DECIMAL", "DECIMAL(38,8)")
    }).mkString(",")

    val jdbcReaderOptions = Map(
      "url" -> jobConf.sourceJdbcUrl,
      "user" -> jobConf.sourceUsername,
      "password" -> jobConf.sourcePassword,
      "query" -> querySql,
      "fetchsize" -> jobConf.fetchSize,
      "pushDownLimit" -> "true",
      "customSchema" -> customSchema,
      "queryTimeout" -> "3600"
    )

    val jdbcWriteOptions = Map(
      "url" -> jobConf.targetJdbcUrl,
      "user" -> jobConf.targetUsername,
      "password" -> jobConf.targetPassword,
      "batchsize" -> jobConf.batchSize,
      "dbtable" -> jobConf.targetTable
    )

    val sourceDF = spark.read
      .format("jdbc")
      .options(jdbcReaderOptions)
      .load()

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
     * 敏感字段加密处理
     */
    finalDF = Supplement.encryptTrans(finalDF, jobConf.encrypt)

    val writer = new JdbcWriter(finalDF, jobConf.writeMode, jdbcWriteOptions)
    writer.write(jobConf.mergeKeys)


    /**
     * 增量读取时，存储数据中offset最大值
     */
    if ("incr".equals(jobConf.readMode)) {
      maxOffsetDf.write.option("header", value = true).mode(SaveMode.Overwrite).csv(offsetPath.format(jobConf.taskID))
    }
  }
}
