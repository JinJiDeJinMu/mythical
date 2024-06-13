package com.nebula.hs.common

import com.hs.utils.CommonFunction.mapElementAdd
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 * @Description {{{
 *   SparkSessionWrapper便于扩展 Properties Through Add Config
 * }}}
 * @author wzw
 * @date 2023/2/7
 */
trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    /**
     * Spark session 配置信息
     */
    val sparkConfMap = Map(
      "spark.app.name" -> appName,
      "spark.master" -> runMode,
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      "spark.sql.legacy.parquet.int96RebaseModeInWrite" -> "CORRECTED"
    )

    /**
     * Delta SparkSession
     */
    SparkSession
      .builder()
      .config(new SparkConf().setAll(mapElementAdd(sparkConfMap, updateConfig)))
      //.enableHiveSupport()
      .getOrCreate()
  }
  val appName = "BaseJob"
  val runMode = "yarn" //local[*]
  val updateConfig = Map[String, String]()
}
