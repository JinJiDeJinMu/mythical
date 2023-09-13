//package com.hs.etl.source
//
//import com.google.gson.JsonObject
//import com.hs.etl.config.EtlConfig
//import com.hs.etl.config.source.EsSourceConfig
//import com.hs.utils.{EsUtil, OffsetUtil, Transformer}
//import org.apache.commons.lang.StringUtils
//import org.apache.spark.sql._
//import org.apache.spark.sql.functions.col
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable.ListBuffer
//
///**
// * @Author ChengJie
// * @Date 2023/5/15
// */
//class EsSource extends EtlSource[EsSourceConfig]{
//
//  override def getData(spark: SparkSession,jobConfig:EtlConfig): Dataset[Row] = {
//
//    var options = Map(
//      //"es.index.auto.create" -> "true",
//      "es.nodes.wan.only" -> "true",
//      "es.resource" -> s"${config.getEsIndex}/${config.getEsType}",
//      "es.query" -> config.getEsQuery,
//      "es.nodes" -> config.getEsNodes.split(":")(0),
//      "es.port" -> config.getEsNodes.split(":")(1),
//      "es.net.http.auth.user" -> config.getUsername,
//      "es.net.http.auth.pass" -> config.getPassword,
//      "es.mapping.date.rich" -> "false"
//    )
//
//    if (StringUtils.isNotBlank(config.getEsConf)) {
//      config.getEsConf.split(",").map(x => x.trim).foreach(each => {
//        options += (each.split("->")(0).trim -> each.split("->")(1).trim)
//      })
//    }
//
//    val esDf = spark
//      .read
//      .format("org.elasticsearch.spark.sql")
//      .options(options)
//      .load()
//
//    /*
//    //解密 fixme: source table name gain
//    Supplement.encryptDecryptTransMul(
//      mutable.Map(" " -> esDf)
//      ,"decrypt",jobConfig.getSource.getAlgConfig.asScala.toList)
//*/
//
//    /**
//     * 从df中读取es 增量
//     */
//    var incrDf = esDf
//    var offsetColumns = config.getOffsetColumns
//    if ("incr".equals(config.getReadMode)) {
//      esDf.createOrReplaceTempView("temp")
//      incrDf = spark.sql(OffsetUtil.getQuerySqlByEsSourceConfig(spark, config))
////      //获取source和target的字段映射关系
////      var columnMapFinal = Map[String, String]()
////      config.getColumnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
////      offsetColumns = columnMapFinal(config.getOffsetColumns)
//    }
//
//    if (StringUtils.isNotBlank(jobConfig.getColumnMap)) {
//      var columnList = ListBuffer[String]()
//      jobConfig.getColumnMap.split(",").foreach(x => columnList += (x.split(":")(0)))
//      incrDf = incrDf.select(columnList.map(c => col(c)): _*)
//    }
//
//    //1、字段名映射转换
//    val esMapping: JsonObject = new EsUtil().getEsMapping(config.getEsNodes.split(":")(0), config.getEsNodes.split(":")(1).toInt,config.getUsername, config.getPassword, config.getEsIndex)
//    val esDfFinal = Transformer.fieldNameAndTypeTransform(incrDf, config.getSourceColumns, jobConfig.getColumnMap,esMapping)
//    esDfFinal
//  }
//}
