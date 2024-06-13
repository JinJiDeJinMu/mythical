//package com.hs.integration
//
//import com.google.gson.JsonObject
//import com.hs.common.SparkCommon
//import com.hs.config.EsToDeltaJobConfig
//import com.hs.utils.DeltaMergeUtil.sortConfig
//import com.hs.utils._
//import org.apache.spark.sql.delta.DeltaLog
//import org.apache.spark.sql.functions.lit
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.json4s.DefaultFormats
//import org.json4s.jackson.JsonMethods
//
//object EsToDelta {
//  val offsetPath = "/delta/_offset/es2stg/%s"
//  val jobType = "Es2Delta"
//
//  def main(args: Array[String]): Unit = {
//    if (args.length == 0) {
//      throw new IllegalArgumentException("任务配置信息异常！")
//    }
//    val params = args(0)
//    println("任务配置信息：" + params)
//
//    implicit val formats: DefaultFormats.type = DefaultFormats
//    val jobConf = JsonMethods.parse(params).extract[EsToDeltaJobConfig]
//
//    /**
//     * spark session 配置信息
//     */
//    val sparkConfig = SparkCommon.sparkConfig
//    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${jobConf.taskID}")
//
//    /**
//     * Delta SparkSession
//     */
//    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)
//
//    //TODO 全局udf
//    //    spark.sql("add jar   ")
//    //    spark.sql("create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'")
//
//    var options = Map(
//      //"es.index.auto.create" -> "true",
//      "es.nodes.wan.only" -> "true",
//      "es.resource" -> s"${jobConf.esIndex}/${jobConf.esType}",
//      "es.query" -> jobConf.esQuery,
//      "es.nodes" -> jobConf.esNodes.split(":")(0),
//      "es.port" -> jobConf.esNodes.split(":")(1),
//      "es.net.http.auth.user" -> jobConf.username,
//      "es.net.http.auth.pass" -> jobConf.password,
//      "es.mapping.date.rich" -> "false"
//    )
//
//    if (jobConf.esConf.nonEmpty) {
//      jobConf.esConf.split(",").map(x => x.trim).foreach(each => {
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
//    /**
//     * 从df中读取es 增量
//     */
//    var incrDf=esDf
//    var maxOffsetDf: DataFrame = null
//    var offsetColumns = jobConf.offsetColumns
//    if ("incr".equals(jobConf.readMode)) {
//      esDf.createOrReplaceTempView("temp")
//      incrDf = spark.sql(OffsetUtil.getEsQuerySql(spark, jobConf))
////      增量读取时，获取数据中offset最大值
//      maxOffsetDf = incrDf.selectExpr(s"max(${jobConf.offsetColumns}) as offset ").withColumn("taskID", lit(jobConf.taskID))
//      //获取source和target的字段映射关系
//      var columnMapFinal = Map[String, String]()
//      jobConf.columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
//      offsetColumns = columnMapFinal(jobConf.offsetColumns)
//    }
//
//    //1、字段名映射转换
//    val esMapping: JsonObject = new EsUtil().getEsMapping(jobConf.esNodes.split(":")(0), jobConf.esNodes.split(":")(1).toInt,jobConf.username, jobConf.password, jobConf.esIndex)
//
//    //2、写入数仓
//    val targetTablePath = jobConf.tablePathFormat.format(jobConf.targetDatabase, jobConf.targetTable)
//    //val targetTable = spark.read.format("delta").load(targetTablePath)
//    val targetTableSchema: StructType = DeltaLog.forTable(spark, targetTablePath).snapshot.schema
////    val targetTableSchema = targetTable.schema
//    incrDf.printSchema()
//    var esDfFinal = Transformer.fieldNameAndTypeTransformEsToDelta(incrDf, jobConf.sourceColumns, jobConf.columnMap,esMapping,targetTableSchema)
//      //.withColumn("insert_time", current_timestamp())
//    esDfFinal.printSchema()
//    /**
//     * 系统字段填充
//     */
//    esDfFinal = Supplement.systemFieldComplete(esDfFinal)
//
//    /**
//     * 分区字段处理
//     */
//    esDfFinal = Supplement.partitionTrans(esDfFinal,jobConf.zoneType,jobConf.zoneFieldCode,jobConf.zoneTargetFieldCode,jobConf.zoneTypeUnit)
//
//    /**
//     * 敏感字段加密处理
//     */
//    esDfFinal = Supplement.encryptTrans(esDfFinal,jobConf.encrypt)
//
////    val deltaSourceDf: DataFrame = spark.read.format("delta").load(targetTablePath)
//
////    esDfFinal = Transformer.fieldNameAndTypeTransformByDeltaSink(deltaSourceDf, esDfFinal)
//
//    val writer = new FrameWriter(esDfFinal, jobConf.writeMode.toLowerCase, Option(null))
//    writer.write(targetTablePath, Option(jobConf.mergeKeys), sortConfig(jobConf.writeMode, offsetColumns))
//
//    /**
//     * 增量读取时，存储数据中offset最大值
//     */
//    if ("incr".equals(jobConf.readMode)) {
//      maxOffsetDf.write.option("header", value = true).mode(SaveMode.Overwrite).csv(offsetPath.format(jobConf.taskID))
//    }
//
//    /**
//     * 接入数据量统计
//     */
//    Supplement.dataCount(jobConf.taskID,"es",esDfFinal.count(),jobConf.dataCountKafkaServers,jobConf.dataCountTopicName,jobConf.targetDatabase)
//  }
//
//}
