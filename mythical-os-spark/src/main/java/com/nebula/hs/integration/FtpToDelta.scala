package com.nebula.hs.integration

import com.hs.common.SparkCommon
import com.hs.config.FtpToDeltaJobConfig
import com.hs.utils.DeltaMergeUtil.orderExpr
import com.hs.utils.{FrameWriter, FtpUtil, Supplement, Transformer}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import java.nio.charset.StandardCharsets

object FtpToDelta {
  val jobType = "Ftp2Delta"
  val FtpTmpPath = "/delta/tmp/csv/"

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[FtpToDeltaJobConfig]

    /**
     * spark session 配置信息
     */
    val sparkConfig = SparkCommon.sparkConfig
    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${jobConf.taskID}")

    /**
     * Delta SparkSession
     */
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)

    val readerOptions = Map(
      "delimiter" -> jobConf.delimiter,
      "header" -> jobConf.includeHeader,
      "encoding" -> jobConf.encoding,
      "multiLine" -> "true",
      "escape" -> "\""
    )

    val ftpClient = new FtpUtil().connectFtp(jobConf.url, jobConf.port, jobConf.username, jobConf.password)
    val filePath = new String(jobConf.filePath.getBytes(jobConf.encoding), StandardCharsets.ISO_8859_1)

    if (!ftpClient.isConnected) {
      throw new RuntimeException("ftp连通失败")
    }

    val files = ftpClient.listFiles(filePath)
    if (files == null || files.length <= 0) {
      throw new RuntimeException("读取的文件不存在")
    }

    val fs = ftpClient.retrieveFileStream(filePath)
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val path = FtpTmpPath + jobConf.taskID + ".csv"
    val fos = fileSystem.create(new Path(path))
    IOUtils.copyBytes(fs, fos, spark.sparkContext.hadoopConfiguration)

    var df = spark.read.options(readerOptions).csv(path)

    /**
     * spark自生成表头处理
     */
    if ("false".equals(jobConf.includeHeader)) {
      for (x <- df.schema) {
        df = df.withColumnRenamed(x.name, x.name.replace("_c", "column"))
      }
    }

    df.printSchema()

    df.createOrReplaceTempView("tmpTable_" + jobConf.taskID)

    var finalDF = spark.sql(Transformer.fieldNameTransform(jobConf.columnMap, jobConf.taskID))
      .withColumn("insert_time", current_timestamp())

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
    val writer = new FrameWriter(finalDF, jobConf.writeMode, Option(null))
    writer.write(targetTablePath, Option(jobConf.mergeKeys), Option(orderExpr(jobConf.sortColumns)))

    /**
     * 接入数据量统计
     */
    Supplement.dataCount(jobConf.taskID, "ftp", finalDF.count(), jobConf.dataCountKafkaServers, jobConf.dataCountTopicName, jobConf.targetDatabase)

    fileSystem.deleteOnExit(new Path(path))
  }
}
