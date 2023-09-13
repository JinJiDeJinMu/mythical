package com.hs.etl.source

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.JdbcSourceConfig
import com.hs.utils._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.jdbc.JdbcDialects


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class JdbcSource extends EtlSource[JdbcSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {
    /**
     * 注册 SqlServer 自定义方言
     */
    JdbcDialects.registerDialect(new MsSqlServerJdbcDialect)

    /**
     * 注册 Oracle 自定义方言
     */
    JdbcDialects.registerDialect(new OracleJdbcDialect)


    /**
     * 注册 Kingbase 自定义方言
     */
    JdbcDialects.registerDialect(new KingbaseJdbcDialect)

    /**
     * 读取hdfs上的offset,拼接查询sql
     */
    val querySql = OffsetUtil.getQuerySqlByJdbcConfig(spark, config)
    println("querySql为：" + querySql)

    /**
     * spark read jdbc options
     */
    val customSchema = config.getSourceColumns.split(",").map(col => {
      "`" + col.split(":")(0) + "` " + col.split(":")(1).replace("DECIMAL", "DECIMAL(38,8)")
    }).mkString(",")

    val jdbcReaderOptions = Map(
      "url" -> config.getJdbcUrl,
      "user" -> config.getUsername,
      "password" -> config.getPassword,
      "query" -> querySql,
      "fetchsize" -> config.getFetchsize,
      "pushDownLimit" -> "true",
      "customSchema" -> customSchema,
      "queryTimeout" -> "3600"
    )

    val sourceDF = spark.read
      .format("jdbc")
      .options(jdbcReaderOptions)
      .load()
      .withColumn("insert_time", current_timestamp())

    /*
        //解密 fixme: source table name gain
        Supplement.encryptDecryptTransMul(
          mutable.Map(" " -> sourceDF)
          ,"decrypt",jobConfig.getSource.getAlgConfig.asScala.toList)
    */

    //    var offsetColumns = config.getOffsetColumns
    //    if ("incr".equals(config.getReadMode)) {
    //      //获取source和target的字段映射关系
    //      var columnMapFinal = Map[String, String]()
    //      config.getColumnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
    //      offsetColumns = columnMapFinal(config.getOffsetColumns)
    //    }

    /**
     * 字段映射转换
     */
    sourceDF.createOrReplaceTempView("tmpTable_" + config.getTaskID)
    val finalDF = spark.sql(Transformer.fieldNameTransform(jobConfig.getColumnMap, config.getTaskID))
    finalDF
  }
}
