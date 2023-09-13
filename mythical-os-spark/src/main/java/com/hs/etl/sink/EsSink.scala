//package com.hs.etl.sink
//
//import com.google.gson.JsonObject
//import com.hs.etl.config.EtlConfig
//import com.hs.etl.config.sink.EsSinkConfig
//import com.hs.utils.{EsUtil, Supplement, Transformer}
//import org.apache.spark.sql.{Dataset, Row, SparkSession}
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//
///**
// * @Author ChengJie
// * @Date 2023/5/15
// */
//class EsSink extends EtlSink[EsSinkConfig] {
//
//  def output(spark: SparkSession, ds: Dataset[Row], jobConfig: EtlConfig,otherInFor:String): Unit = {
//
//    val esMapping: JsonObject = new EsUtil().getEsMapping(config.getEsNodes.split(":")(0), config.getEsNodes.split(":")(1).toInt,config.getUsername, config.getPassword, config.getEsIndex)
//
//
//    val sourceColumns = jobConfig.getSource.getConfig.getAsJsonObject.get("sourceColumns").getAsString
//
//    // drop 非选字段
//    val selectCol = mutable.ArrayBuffer[String]()
//    otherInFor.split(",").foreach(x => {
//      selectCol+=(x.split(":")(1))
//    })
//
//    val esDf = Transformer.fieldNameAndTypeTransformByEs(ds.selectExpr(selectCol: _*), sourceColumns, config.getColumnMap, true, esMapping,config.getEsJsonColumns)
//
//    var esWriteOptions = Map(
//      //      "es.index.auto.create" -> "true",
//      "es.nodes.wan.only" -> "true",
//      "es.nodes" -> config.getEsNodes.split(":")(0),
//      "es.port" -> config.getEsNodes.split(":")(1),
//      "es.net.http.auth.user" -> config.getUsername,
//      "es.net.http.auth.pass" -> config.getPassword,
//      //      "es.mapping.date.rich" -> "false"
//    )
//
//    //    ds.saveToEs(s"${config.getEsIndex}/${config.getEsType}",esWriteOptions)
//
//    if (config.getWriteMode.toLowerCase().equals("update")) {
//      esWriteOptions = esWriteOptions ++ Map(
//        "es.write.operation" -> "upsert",
//        "es.mapping.id" -> config.getMergeKeys
//      )
//      config.setWriteMode("append")
//    }
//
//    //加密 fixme: target
//    val targetTable = jobConfig.getSink.getConfig.getAsJsonObject.get("esIndex").getAsString
//    var sinkDF = Supplement.encryptDecryptTransMul(
//      mutable.Map(targetTable -> esDf)
//      , "encrypt", jobConfig.getSource.getAlgConfig.asScala.toList)(targetTable)
//
//    //内置水印
//    sinkDF = watermarkI(mutable.Map(targetTable -> sinkDF), jobConfig)(targetTable)
//
//    sinkDF.write.format("org.elasticsearch.spark.sql")
//      .options(esWriteOptions)
//      .mode(config.getWriteMode)
//      .save(s"${config.getEsIndex}/${config.getEsType}")
//
//
//    /* offset 存储 */
//    saveOffsetI(ds, jobConfig)
//    /* 入湖数据量统计 */
//    dataCountI(sinkDF, jobConfig)
//  }
//}
