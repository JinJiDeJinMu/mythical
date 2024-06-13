//package com.hs.integration
//
//import com.hs.common.SparkCommon
//import com.hs.config.MongoToDeltaJobConfig
//import com.hs.utils.DeltaMergeUtil.sortConfig
//import com.hs.utils.{FrameWriter, OffsetUtil, Supplement, Transformer}
//import org.apache.spark.sql.functions.{current_timestamp, lit}
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.json4s.DefaultFormats
//import org.json4s.jackson.JsonMethods
//
//object MongoToDelta {
//  val jobType = "Mongo2Delta"
//  val offsetPath = "/delta/_offset/mongo2stg/%s"
//
//  def main(args: Array[String]): Unit = {
//    if (args.length == 0) {
//      throw new IllegalArgumentException("任务配置信息异常！")
//    }
//
//    val params = args(0)
//    println("任务配置信息：" + params)
//
//    implicit val formats: DefaultFormats.type = DefaultFormats
//    val jobConfig = JsonMethods.parse(params).extract[MongoToDeltaJobConfig]
//
//    /**
//     * spark session 配置信息
//     */
//    val sparkConfig = SparkCommon.sparkConfig
//    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${jobConfig.taskID}")
//    sparkConfig.setIfMissing("spark.sql.session.timeZone", "UTC")
//    sparkConfig.setIfMissing("spark.mongodb.input.uri", jobConfig.url)
//    sparkConfig.setIfMissing("spark.mongodb.input.database", jobConfig.sourceDatabase)
//    sparkConfig.setIfMissing("spark.mongodb.input.collection", jobConfig.collection)
//
//    /**
//     * Delta SparkSession
//     */
//    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)
//
//    /**
//     * source->delta映射关系：id:id,name:name,age:age
//     */
//    val sourceTypeMappings: Array[String] = jobConfig.columnMap.split(",")
//
//    /**
//     * 查询sql语句
//     */
//    val preSql = jobConfig.sql
//
//    /**
//     * 库名
//     */
//    val sourceDatabase = jobConfig.sourceDatabase
//
//    /**
//     * 源数据集
//     */
//    val collection = jobConfig.collection
//
//    /**
//     * 增量插入字段: id,update_time
//     */
//    var offsetColumns = jobConfig.offsetColumns
//
//    /**
//     * sink端表路径
//     */
//    val targetTablePath = jobConfig.tablePathFormat.format(jobConfig.targetDatabase,jobConfig.targetTable)
//
//    /** TODO:
//     * 读取hdfs上的offset,拼接查询sql
//     */
//    val querySql = OffsetUtil.getMongoQuerySql(spark,jobConfig)
//    println("querySql为：" + querySql)
//
//    /**
//     * MongoSpark 配置信息
//     *    【注】：ReadConfig 和 WriteConfig 设置会覆盖 SparkConf 中的任何相应设置
//     */
//    val readConfig = ReadConfig(
//      Map(
//        "database" -> sourceDatabase,
//        "collection" -> collection,
//        "readPreference.name" -> "secondaryPreferred"
//      ),
//      Some(ReadConfig(spark))
//    )
//
//    var mdf: DataFrame = MongoSpark.load(spark, readConfig)
//    mdf.createOrReplaceTempView(collection)
//    mdf = Transformer.fieldNameAndTypeTransformByMongo(spark.sql(querySql),jobConfig.sourceColumns, jobConfig.columnMap)
//
//    /** TODO:
//     * 增量读取时，获取数据中offset最大值
//     */
//    var maxOffsetDf: DataFrame = null
//    if ("incr".equals(jobConfig.readMode)) {
//      maxOffsetDf = mdf.selectExpr(s"max(${jobConfig.offsetColumns}) as offset ").withColumn("taskID", lit(jobConfig.taskID))
//
//      //获取source和target的字段映射关系
//      var columnMapFinal = Map[String, String]()
//      jobConfig.columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
//      offsetColumns = columnMapFinal(jobConfig.offsetColumns)
//    }
//    /**
//     * sqlQuery: 根据传入的sql是否为""选怎处理方式,默认值: sql=""
//     *    eg. select * from collectionName where ...
//     */
//    //if (querySql!=""){
//      //mdf =Transformer.transform(mdf.sqlContext.sql(querySql))
//    //}else{
//    //mdf = Transformer.transform(mdf)
//    //mdf.createOrReplaceTempView(viewName)
//    //mdf = spark.sql(generateSql(sourceTypeMappings, viewName)).toDF()
//    //}
//    mdf = mdf.withColumn("insert_time", current_timestamp())
//    //mdf.printSchema()
//    //mdf.show(false)
//
//    /**
//     * 系统字段填充
//     */
//    mdf = Supplement.systemFieldComplete(mdf)
//
//    /**
//     * 分区字段处理
//     */
//    mdf = Supplement.partitionTrans(mdf,jobConfig.zoneType,jobConfig.zoneFieldCode,jobConfig.zoneTargetFieldCode,jobConfig.zoneTypeUnit)
//
//    /**
//     * 敏感字段加密处理
//     */
//    mdf = Supplement.encryptTrans(mdf,jobConfig.encrypt)
//
//    // 入湖
//    val writer = FrameWriter(mdf, jobConfig.writeMode.toLowerCase, Option(null))
//    // FIXME: offsetColumns 传入?
//    //writer.write(targetTablePath, Option(jobConfig.mergeKeys), Option(orderExpr(jobConfig.sortColumns)))
//    writer.write(targetTablePath, Option(jobConfig.mergeKeys), sortConfig(jobConfig.writeMode, offsetColumns))
//
//    /** TODO:
//     * 增量读取时，存储数据中offset最大值
//     */
//    if ("incr".equals(jobConfig.readMode)) {
//      maxOffsetDf.write.option("header", value = true).mode(SaveMode.Overwrite).csv(offsetPath.format(jobConfig.taskID))
//    }
//
//    /**
//     * 接入数据量统计
//     */
//    Supplement.dataCount(jobConfig.taskID,"mongodb",mdf.count(),jobConfig.dataCountKafkaServers,jobConfig.dataCountTopicName,jobConfig.targetDatabase)
//  }
//
//  /**
//   *  select `cmd` as cmd, `description` as description,`name` as name from collection
//   * @param sourceTypeMappings  [columnMap.split(",")]
//   * @param viewName            ["tmpTable_" + jobConfig.taskID]
//   * @return
//   */
//  def generateSql(sourceTypeMappings: Array[String], viewName: String): String = {
//    val stringBuilder = new StringBuilder
//    stringBuilder.append("select ")
//    val str: String = sourceTypeMappings.map(column => {
//      val split = column.split(":")
//      "`" + split(0) + "`" + " as " + split(1)
//    }).mkString(", ")
//    stringBuilder.append(str).append(" from ").append(viewName).toString()
//  }
//}
