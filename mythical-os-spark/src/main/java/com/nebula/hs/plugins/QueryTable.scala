package com.nebula.hs.plugins

import com.hs.common.SparkSessionWrapper
import com.hs.config.SystemConfig
import com.hs.plugins.JobUtil.{createDataFrame, createTempView}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.collection.mutable


case class QueryTableConfig(sourceDbTableMap: String, sql: String)

/**
 * {"sourceDbTableMap": "stg:delta_history_data_count_20230621","sql": "select dt,sum(cnt) as dt_sum from delta_history_data_count_20230621 group by dt"}
 *
 * @Description 集群查询表
 * @author wzw
 * @date 2023/6/19
 */
object QueryTable extends SparkSessionWrapper {
  override val appName: String = "QueryDeltaTable"
  //override val runMode: String = "local[*]"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[QueryTableConfig]
    println(jobConf)

    // load table
    val sourceMapDf = createDataFrame(spark, jobConf.sourceDbTableMap)

    // table temp view
    createTempView(sourceMapDf)

    //getDatabaseTableMap(jobConf.sourceDbTableMap)

    spark.sql(jobConf.sql).show(false)
  }


}

// job util
object JobUtil {
  /**
   * DeltaTable 路径格式
   */
  val tablePathFormat: String = SystemConfig.tablePathFormat

  /**
   * database -> table Map
   */
  def getDatabaseTableMap(dbTableMap: String): mutable.Map[String, String] = {
    val sourceDbTbMap: mutable.Map[String, String] = mutable.Map[String, String]()
    dbTableMap.split(",").foreach(source => {
      val split = source.split(":")
      sourceDbTbMap += (split(0) -> split(1))
    })
    sourceDbTbMap
  }

  /**
   * SparkSql 读取指定路径的 deltaTable
   */
  def createDataFrame(spark: SparkSession
                      , sourceDbTableMap: String): mutable.Map[String, DataFrame] = {
    val sourceTablePathMap = generateDeltaTablePath(sourceDbTableMap)
    val sourceTableDataFrameMap = mutable.Map[String, DataFrame]()
    sourceTablePathMap.foreach(kv => {
      val sourceTableName = kv._1
      val sourceDeltaTable = kv._2
      sourceTableDataFrameMap += (
        sourceTableName ->
          spark.read.format("delta").load(sourceDeltaTable)
        )
    })
    sourceTableDataFrameMap
  }

  /**
   * 生成 delta 表的有效路径
   */
  private def generateDeltaTablePath(dbTableMap: String): mutable.Map[String, String] = {
    val tarTablePathMap = mutable.Map[String, String]()
    dbTableMap.split(",").foreach(source => {
      val split = source.split(":")
      tarTablePathMap += (
        split(1) ->
          tablePathFormat.format(split(0), split(1))
        )
    })
    tarTablePathMap
  }

  /**
   * 创建SparkSession级的临时视图
   */
  def createTempView(dataFrameMap: mutable.Map[String, DataFrame]): Unit = {
    dataFrameMap.foreach(kv => {
      kv._2.createOrReplaceTempView(kv._1)
    })
  }
}