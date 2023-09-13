package com.hs.etl.sink

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.sink.HiveSinkConfig
import com.hs.utils.Supplement
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.DriverManager
import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class HiveSink extends EtlSink[HiveSinkConfig] {

  def output(spark: SparkSession, ds: Dataset[Row], jobConfig: EtlConfig, otherInFor: String): Unit = {
    // drop 非选字段
    val selectCol = mutable.ArrayBuffer[String]()
    otherInFor.split(",").foreach(x => {
      selectCol += (x.split(":")(1))
    })
    //加密
    val targetTable = jobConfig.getSink.getConfig.getAsJsonObject.get("targetTable").getAsString
    //val targetTable = config.getTargetTable
    var sinkDF = Supplement.encryptDecryptTransMul(
      mutable.Map(targetTable -> ds.selectExpr(selectCol: _*))
      , "encrypt", jobConfig.getSource.getAlgConfig.asScala.toList)(targetTable)


    //内置水印
    sinkDF = watermarkI(mutable.Map(targetTable -> sinkDF), jobConfig)(targetTable)

    sinkDF.write
      .mode(config.getWriteMode)
      .format("parquet")
      //.option("permission", FsPermission.createImmutable(777.toShort).toString)
      .save(config.getHiveHdfsPath)

    /**
     * hive jdbc 连接配置
     */
    val conn = {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      DriverManager.getConnection(config.getJdbcUrl, config.getUsername, config.getPassword)
    }

    val statement = conn.prepareStatement(s"load data inpath  \'${config.getHiveHdfsPath}\' into table  ${config.getTargetTable} ")
    val result = statement.execute()

    /* offset 存储 */
    saveOffsetI(ds, jobConfig)
    /* 入湖数据量统计 */
    dataCountI(sinkDF, jobConfig)
  }
}
