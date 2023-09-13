package com.hs.etl.source

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.DeltaSourceConfig
import com.hs.utils.{OffsetUtil, Supplement, Transformer}
import org.apache.spark.sql._

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class DeltaSource extends EtlSource[DeltaSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {

    //获取delta df
    var df = spark.read.format("delta").load(config.getTablePathFormat.format(config.getSourceDatabase.toLowerCase(), config.getSourceTable))

    //解密
    Supplement.encryptDecryptTransMul(
      mutable.Map(config.getSourceTable -> df)
      , "decrypt", jobConfig.getSource.getAlgConfig.asScala.toList)

    //查询过滤条件
    val sql = OffsetUtil.getDeltaQuerySqlByDeltaSourceConfig(spark, config)
    println("querySql=" + sql)
    if (sql != null && sql.nonEmpty) {
      df.createOrReplaceTempView("tmp_" + config.getTaskID)
      df = spark.sql(sql)
    }

    df.createOrReplaceTempView("tmpTable_" + config.getTaskID)

    val finalDF = spark.sql(Transformer.fieldNameTransform(jobConfig.getColumnMap, config.getTaskID))

    finalDF
  }
}
