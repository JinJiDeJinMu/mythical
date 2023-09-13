package com.hs.etl.source

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.JdbcSourceConfig
import com.hs.utils.{HiveJdbcDialet, OffsetUtil, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.jdbc.JdbcDialects


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class HiveSource extends EtlSource[JdbcSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {
    /**
     * Register hive jdbc dialect
     */
    JdbcDialects.registerDialect(new HiveJdbcDialet)

    /**
     * 读取hdfs上的offset,拼接查询sql
     */
    val querySql = OffsetUtil.getQuerySqlByJdbcConfig(spark, config)
    println("querySql为：" + querySql)

    var hiveJdbcUrl = config.getJdbcUrl
    val hiveUniqueColumnConfig = "hive.resultset.use.unique.column.names=false"
    hiveJdbcUrl = if (hiveJdbcUrl.contains("?")) {
      hiveJdbcUrl + "&" + hiveUniqueColumnConfig
    } else {
      hiveJdbcUrl + "?" + hiveUniqueColumnConfig
    }

    /**
     * spark read jdbc options
     */
    val jdbcReaderOptions = Map(
      "url" -> hiveJdbcUrl,
      "user" -> config.getUsername,
      "password" -> config.getPassword,
      "query" -> querySql,
      "fetchsize" -> config.getFetchsize,
      "pushDownLimit" -> "true"
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


    var offsetColumns = config.getOffsetColumns
    if ("incr".equals(config.getReadMode)) {
      //获取source和target的字段映射关系
      //      var columnMapFinal = Map[String, String]()
      //      config.getColumnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
      //      offsetColumns = columnMapFinal(config.getOffsetColumns)
    }

    /**
     * 字段映射转换
     */
    sourceDF.createOrReplaceTempView("tmpTable_" + config.getTaskID)
    val finalDF = spark.sql(Transformer.fieldNameTransform(config.getColumnMap, config.getTaskID))
    finalDF
  }
}
