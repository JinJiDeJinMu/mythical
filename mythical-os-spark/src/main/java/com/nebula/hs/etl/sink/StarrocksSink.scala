package com.nebula.hs.etl.sink

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.sink.StarrocksSinkConfig
import com.hs.utils.Supplement
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class StarrocksSink extends EtlSink[StarrocksSinkConfig] {

  def output(spark: SparkSession, ds: Dataset[Row], jobConfig: EtlConfig, otherInFor: String): Unit = {

    val writeOptions = Map(
      "spark.starrocks.write.fe.urls.http" -> config.getHttpUrl,
      "spark.starrocks.conf" -> "write",
      "spark.starrocks.write.fe.urls.jdbc" -> config.getJdbcUrl,
      "spark.starrocks.write.username" -> config.getUsername,
      "spark.starrocks.write.password" -> config.getPassword,
      "spark.starrocks.write.properties.ignore_json_size" -> "true",
      "starrocks.sink.batch.size" -> config.getBatchsize,
      "spark.starrocks.write.database" -> config.getTargetTable.split("\\.")(0),
      "spark.starrocks.write.table" -> config.getTargetTable.split("\\.")(1)
    )
    // drop 非选字段
    val selectCol = mutable.ArrayBuffer[String]()
    otherInFor.split(",").foreach(x => {
      selectCol += (x.split(":")(1))
    })

    //加密
    val targetTable = jobConfig.getSink.getConfig.getAsJsonObject.get("targetTable").getAsString.split("\\.")(1)
    var sinkDF = Supplement.encryptDecryptTransMul(
      mutable.Map(targetTable -> ds.selectExpr(selectCol: _*))
      , "encrypt", jobConfig.getSource.getAlgConfig.asScala.toList)(targetTable)

    //内置水印
    sinkDF = watermarkI(mutable.Map(targetTable -> sinkDF), jobConfig)(targetTable)

    sinkDF.write.format("starrocks_writer")
      .options(writeOptions)
      .mode(config.getWriteMode)
      .save()

    /* offset 存储 */
    saveOffsetI(ds, jobConfig)
    /* 入湖数据量统计 */
    dataCountI(sinkDF, jobConfig)
  }
}
