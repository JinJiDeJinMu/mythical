//package com.hs.pipeline
//
//import com.hs.config.DeltaToJdbcConfigV2
//import com.hs.utils.{JdbcWriter, OffsetUtil, Transformer}
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.functions.lit
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.json4s.DefaultFormats
//import org.json4s.jackson.JsonMethods
//
//
//object DeltaToJdbc {
//  val offsetPath = "/delta/_offset/delta2jdbc/%s"
//
//  def main(args: Array[String]): Unit = {
//    if (args.length == 0) {
//      throw new IllegalArgumentException("任务配置信息异常!")
//    }
//
//    val params = args(0)
//    println("任务配置信息：" + params)
//
//    implicit val formats: DefaultFormats.type = DefaultFormats
//    val deltaToJdbcConfig = JsonMethods.parse(params).extract[DeltaToJdbcConfigV2]
//
//    /**
//     * spark session 配置信息
//     */
//    val sparkConfMap = Map(
//      "spark.app.name" -> s"Jdbc-to-delta-${deltaToJdbcConfig.taskID}",
//      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
//      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
//    )
//
//    val jdbcWriteOptions = Map(
//      "url" -> deltaToJdbcConfig.jdbcUrl,
//      "user" -> deltaToJdbcConfig.username,
//      "password" -> deltaToJdbcConfig.password,
//      "batchsize" -> deltaToJdbcConfig.batchSize,
//      "dbtable" -> s"${deltaToJdbcConfig.targetDatabase}.${deltaToJdbcConfig.targetTable}"
//    )
//
//    /**
//     * Delta SparkSession
//     */
//    val spark: SparkSession = {
//      SparkSession
//        .builder()
//        //.master("local")
//        .config(new SparkConf().setAll(sparkConfMap))
//        .getOrCreate()
//    }
//
//    //获取delta df
//    var df = spark.read.format("delta").load(deltaToJdbcConfig.tablePathFormat.format(deltaToJdbcConfig.sourceDatabase, deltaToJdbcConfig.sourceTable))
//
//    //查询过滤条件
//    val sql = OffsetUtil.getDeltaQuerySql(spark, deltaToJdbcConfig)
//    println("querySql=" + sql)
//    if (sql != null && sql.nonEmpty) {
//      df.createOrReplaceTempView("tmp_" + deltaToJdbcConfig.taskID)
//      df = spark.sql(sql)
//    }
//
//    df.createOrReplaceTempView("tmpTable_" + deltaToJdbcConfig.taskID)
//
//    val finalDF = spark.sql(Transformer.fieldNameTransform(deltaToJdbcConfig.columnMap, deltaToJdbcConfig.taskID))
//
//    /**
//     * 增量读取时，获取数据中offset最大值
//     */
//    var maxOffsetDf: DataFrame = null
//    var offsetColumns = deltaToJdbcConfig.offsetColumns
//    if ("incr".equals(deltaToJdbcConfig.readMode)) {
//      maxOffsetDf = finalDF.selectExpr(s"max(${deltaToJdbcConfig.offsetColumns}) as offset ").withColumn("taskID", lit(deltaToJdbcConfig.taskID))
//
//      //获取source和target的字段映射关系
//      var columnMapFinal = Map[String, String]()
//      deltaToJdbcConfig.columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
//      offsetColumns = columnMapFinal(deltaToJdbcConfig.offsetColumns)
//    }
//
//    val writer = new JdbcWriter(finalDF, deltaToJdbcConfig.writeMode, jdbcWriteOptions)
//    writer.write(deltaToJdbcConfig.mergeKeys)
//
//    //更新offset
//    if ("incr".equals(deltaToJdbcConfig.readMode)) {
//      maxOffsetDf.write.option("header", value = true).mode(SaveMode.Overwrite).csv(offsetPath.format(deltaToJdbcConfig.taskID))
//    }
//  }
//
//}
