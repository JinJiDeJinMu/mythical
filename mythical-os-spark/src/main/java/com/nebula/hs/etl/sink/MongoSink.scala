package com.nebula.hs.etl.sink

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.sink.MongoSinkConfig
import com.hs.utils.Supplement
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class MongoSink extends EtlSink[MongoSinkConfig] {

  def output(spark: SparkSession, ds: Dataset[Row], jobConfig: EtlConfig, otherInFor: String): Unit = {

    //    //数据写入Mongo
    //    val writeConfig = WriteConfig.create(config.getTargetDatabase, //数据库名
    //      config.getCollection, //表名
    //      config.getUrl, //uri
    //      2, //线程数
    //      WriteConcern.ACKNOWLEDGED)
    //
    ////    MongoSpark.save(ds, config)
    //
    //    ds.write.mode(config.getWriteMode).mongo(writeConfig)

    val options = Map(
      "spark.mongodb.output.database" -> config.getTargetDatabase,
      "spark.mongodb.output.collection" -> config.getCollection,
      "spark.mongodb.output.uri" -> config.getUrl,
      "readPreference.name" -> "secondaryPreferred"
    )
    // drop 非选字段
    val selectCol = mutable.ArrayBuffer[String]()
    otherInFor.split(",").foreach(x => {
      selectCol += (x.split(":")(1))
    })

    //加密
    val targetCollection = jobConfig.getSink.getConfig.getAsJsonObject.get("collection").getAsString
    var sinkDF = Supplement.encryptDecryptTransMul(
      mutable.Map(targetCollection -> ds.selectExpr(selectCol: _*))
      , "encrypt", jobConfig.getSource.getAlgConfig.asScala.toList)(targetCollection)


    //内置水印
    sinkDF = watermarkI(mutable.Map(targetCollection -> sinkDF), jobConfig)(targetCollection)

    sinkDF.write.format("mongo").options(options).mode(config.getWriteMode).save()

    /* offset 存储 */
    saveOffsetI(ds, jobConfig)
    /* 入湖数据量统计 */
    dataCountI(sinkDF, jobConfig)
  }
}
