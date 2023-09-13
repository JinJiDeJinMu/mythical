package com.hs.etl.sink

import com.alibaba.fastjson.JSON
import com.hs.etl.config.EtlConfig
import com.hs.etl.config.sink.JdbcSinkConfig
import com.hs.utils.{FsJdbcUtils, JdbcWriter, Supplement}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class JdbcSink extends EtlSink[JdbcSinkConfig] {

  protected lazy implicit val logger = LoggerFactory.getLogger(getClass)

  val tempTablePrefix = "tmpTable_"
  val sqlTemplate = "select * from %s where %s > %s"

  def output(spark: SparkSession, ds: Dataset[Row], jobConfig: EtlConfig, otherInFor: String): Unit = {

    // drop 非选字段
    val selectCol = mutable.ArrayBuffer[String]()
    otherInFor.split(",").foreach(x => {
      selectCol += (x.split(":")(1))
    })
    var sinkDF = ds.selectExpr(selectCol: _*)

    val jdbcWriteOptions = Map(
      "url" -> config.getJdbcUrl,
      "user" -> config.getUsername,
      "password" -> config.getPassword,
      "batchsize" -> config.getBatchsize,
      "dbtable" -> config.getTargetTable
    )

    //加密
    val targetTable = jobConfig.getSink.getConfig.getAsJsonObject.get("targetTable").getAsString
    sinkDF = Supplement.encryptDecryptTransMul(
      mutable.Map(targetTable -> sinkDF)
      , "encrypt", jobConfig.getSource.getAlgConfig.asScala.toList)(targetTable)


    //内置水印
    sinkDF = watermarkI(mutable.Map(targetTable -> sinkDF), jobConfig)(targetTable)

    val breakpointResume = java.lang.Boolean.valueOf(config.getBreakpointResume)

    if (breakpointResume && !config.getJdbcUrl.startsWith("jdbc:postgresql") && !config.getWriteMode.toLowerCase().equals("update")) {
      throw new RuntimeException("JdbcSink断点续传开启失败，目前仅支持sink端为postgresql，并且'writeMode'为'update',请修改配置")
    }

    var enableBreakpointResume: Boolean = false;
    if (breakpointResume && config.getJdbcUrl.startsWith("jdbc:postgresql") && config.getWriteMode.toLowerCase().equals("update")) {
      //判断是不是第一次执行断点续传任务
      val deltaOffsetPath = FsJdbcUtils.breakpointResumeOffsetPath.format(jobConfig.getTaskID)
      val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(deltaOffsetPath))

      //如果不是第一次执行断点续传任务，获取offset，注册临时表，拼接where条件
      //sql类似：select * from table where breakpointResumeColumn > offset
      if (offsetPathExists && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(deltaOffsetPath)).nonEmpty) {
        val offsetJson = spark.read.option("header", value = true).csv(deltaOffsetPath).toJSON.collectAsList().get(0)
        if (JSON.parseObject(offsetJson).get(config.getBreakpointResumeColumn) != null) {
          val offsetValue = JSON.parseObject(offsetJson).get(config.getBreakpointResumeColumn).toString
          val temTableName: String = tempTablePrefix + jobConfig.getTaskID
          sinkDF.createOrReplaceTempView(temTableName)

          val sql: String = String.format(sqlTemplate, temTableName, config.getBreakpointResumeColumn, offsetValue)
          sinkDF = spark.sql(sql)
          logger.info("断点续传sql为: {}", sql)
        }
      }

      logger.info("pg断点续传开启，数据开始排序，使用'{}'作为排序字段", config.getBreakpointResumeColumn)
      val sortStartTime: Long = System.currentTimeMillis()
      sinkDF = sinkDF.sort(config.getBreakpointResumeColumn)
      val sortStopTime: Long = System.currentTimeMillis()
      logger.info("pg断点续传数据排序结束,耗时：{}秒", (sortStopTime - sortStartTime) / 1000)
      enableBreakpointResume = true
    }
    val writer = new JdbcWriter(sinkDF, config.getWriteMode, jdbcWriteOptions, enableBreakpointResume, config.getBreakpointResumeColumn, spark, jobConfig.getTaskID)
    writer.write(config.getMergeKeys)

    /* offset 存储 */
    saveOffsetI(ds, jobConfig)
    /* 入湖数据量统计 */
    dataCountI(sinkDF, jobConfig)
  }
}
