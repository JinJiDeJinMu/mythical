/*
 *@Author   : DoubleTrey
 *@Time     : 2022/10/24 14:00
 */

package com.hs

import com.hs.config.DeltaJobConfig
import com.hs.utils.DeltaMergeUtil._
import com.hs.utils.FrameWriter
import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Delta 表数据处理
 * == Example ==
 *
 * {{{
 *   import com.hs.DeltaJob
 *   import com.hs.config.DeltaJobConfig
 *
 *   val jobConf = DeltaJobConfig(...)
 *   val job = DeltaJob(jobConf)
 *   val transformedDF = job.transform(
 *      """
 *        |SELECT
 *        | *
 *        |FROM source_tbl
 *        |""".stripMargin)
 *   job.saveToDelta(transformedDF)
 * }}}
 *
 * @param jobConf [[DeltaJobConfig]]
 */
case class DeltaJob(jobConf: DeltaJobConfig) {
  val sourceAlias = "s"
  val targetAlias = "t"
  val sourceView = "source_tbl"

  val appName: String = jobConf.appName
  val tablePathFormat: String = jobConf.tablePathFormat
  val sourceDB: String = jobConf.sourceDB
  val sourceTableName: String = jobConf.sourceTable
  val targetDB: String = jobConf.targetDB
  val targetTable: String = jobConf.targetTable
  val writeMode: String = jobConf.writeMode
  val mergeKeys: String = jobConf.mergeKeys
  val sortColumns: String = jobConf.sortColumns
  /**
   * Delta SparkSession
   */
  val spark: SparkSession = {
    SparkSession
      .builder()
      //.appName(appName)
      .master("local[*]")
      .config(new SparkConf().setAll(sparkConfMap))
      .enableHiveSupport()
      .getOrCreate()
  }
  /**
   * 根据 jobConfig 获取来源表 DataFrame
   */
  val sourceDF: DataFrame = {
    // TODO: 支持数据筛选，where update_time >= $last_job_offset and update_time < $curr_job_offset
    DeltaTable.forPath(spark, tablePathFormat.format(sourceDB, sourceTableName)).toDF
  }
  /**
   * spark session 配置信息
   */
  private val sparkConfMap = Map(
    "spark.app.name" -> appName,
    //    "spark.master" -> "local[*]",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
  )

  def transform(sql: String): DataFrame = {
    sourceDF.createOrReplaceTempView(sourceView)
    spark.sql(sql)
  }

  /**
   * 根据写入方式，将输入的 DataFrame 写至目标表
   *
   * @param dataFrame [[DataFrame]]
   */
  def saveToDelta(dataFrame: DataFrame): Unit = {
    val writer = new FrameWriter(sourceDF, writeMode.toLowerCase, Option(null))
    writer.write(tablePathFormat.format(targetDB, targetTable), Option(mergeKeys), Option(orderExpr(sortColumns)))
  }
}