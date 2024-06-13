package com.nebula.hs.etl.source

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.FtpSourceConfig
import com.hs.integration.FtpToDelta.FtpTmpPath
import com.hs.utils.{FtpUtil, Supplement, Transformer}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions.current_timestamp

import java.nio.charset.StandardCharsets


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class FtpSource extends EtlSource[FtpSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {
    val readerOptions = Map(
      "delimiter" -> config.getDelimiter,
      "header" -> config.getIncludeHeader,
      "encoding" -> config.getEncoding,
      "multiLine" -> "true",
      "escape" -> "\""
    )

    val ftpClient = new FtpUtil().connectFtp(config.getUrl, config.getPort, config.getUsername, config.getPassword)
    val filePath = new String(config.getFilePath.getBytes(config.getEncoding), StandardCharsets.ISO_8859_1)

    if (!ftpClient.isConnected) {
      throw new RuntimeException("ftp连通失败")
    }

    val files = ftpClient.listFiles(filePath)
    if (files == null || files.length <= 0) {
      throw new RuntimeException("读取的文件不存在")
    }

    val fs = ftpClient.retrieveFileStream(filePath)
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val path = FtpTmpPath + config.getTaskID + ".csv"
    val fos = fileSystem.create(new Path(path))
    IOUtils.copyBytes(fs, fos, spark.sparkContext.hadoopConfiguration)

    var df = spark.read.options(readerOptions).csv(path)

    /**
     * spark自生成表头处理
     */
    if ("false".equals(config.getIncludeHeader)) {
      for (x <- df.schema) {
        df = df.withColumnRenamed(x.name, x.name.replace("_c", "column"))
      }
    }

    df.printSchema()

    df.createOrReplaceTempView("tmpTable_" + config.getTaskID)

    var finalDF = spark.sql(Transformer.fieldNameTransform(config.getColumnMap, config.getTaskID))
      .withColumn("insert_time", current_timestamp())

    /**
     * 系统字段填充
     */
    finalDF = Supplement.systemFieldComplete(finalDF)

    finalDF
  }
}
