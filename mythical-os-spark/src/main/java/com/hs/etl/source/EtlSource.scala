package com.hs.etl.source

import com.hs.etl.EtlPlugin
import com.hs.etl.config.EtlConfig
import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
trait EtlSource[T <: EtlConfig] extends EtlPlugin[T] {
  def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row]
}
