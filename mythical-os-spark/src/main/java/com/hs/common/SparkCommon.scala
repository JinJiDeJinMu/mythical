/*
 *@Author   : DoubleTrey
 *@Time     : 2023/2/14 16:21
 */

package com.hs.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkCommon {
  /**
   * spark session 配置信息
   */
  val sparkConfMap: Map[String, String] = Map(
    // for DEV
    "spark.master" -> "local[*]",

    // Delta lake
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    "spark.sql.parquet.int96RebaseModeInWrite" -> "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
    "spark.sql.streaming.stateStore.stateSchemaCheck" -> "false"
  )

  val sparkConfig: SparkConf = new SparkConf().setAll(sparkConfMap)

  def loadSession(config: SparkConf): SparkSession = {
    SparkSession
      .builder()
      .config(config)
      //.enableHiveSupport()
      .getOrCreate()
  }
}
