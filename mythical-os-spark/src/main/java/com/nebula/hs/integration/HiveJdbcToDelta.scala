package com.nebula.hs.integration

import com.alibaba.fastjson._
import com.hs.common.{DataType, SparkCommon}
import com.hs.config.HiveToDeltaJobConfig
import com.hs.udfs.partitioner.{PartitionFactory, TPartition}
import com.hs.utils.FrameWriter
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.util
import scala.collection.mutable.ListBuffer

@deprecated
object HiveJdbcToDelta {
  val jobType = "Hive2Delta"

  def main(args: Array[String]): Unit = {
    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConfig = JsonMethods.parse(params).extract[HiveToDeltaJobConfig]

    /**
     * spark session 配置信息
     */
    val sparkConfig = SparkCommon.sparkConfig
    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${jobConfig.taskID}")

    /**
     * Delta SparkSession
     */
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)

    /**
     * hive jdbc 连接配置
     */
    val conn = {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      DriverManager.getConnection(jobConfig.jdbcUrl, jobConfig.username, jobConfig.password)
    }

    /**
     * source->delta映射关系：id:id,name:name,age:age
     */
    val sourceTypeMappings: Array[String] = jobConfig.columnMap.split(",")
    /**
     * delta表字段类型：id:int,name:string,age:int
     */
    val sinkTypeMappings: Array[String] = jobConfig.sinkColumnsType.split(" , ")
    val sinkTypeMap: Map[String, String] = sinkTypeMappings.map(mapping => {
      val split = mapping.split(":")
      (split(0), split(1))
    }).toMap[String, String]

    /**
     * 源表字段类型：ID:int,NAME:string,AGE:int
     */
    val sourceTypeMap: Map[String, String] = generateSinkTypeMappings(sourceTypeMappings, sinkTypeMap)

    /**
     * 查询sql语句
     */
    val sql = jobConfig.sql

    /**
     * 源表名
     */
    val sourceTable = jobConfig.sourceTable

    /**
     * 增量插入字段: id,update_time
     */
    val offsetColumns = jobConfig.offsetColumns

    /**
     * 写入delta的表路径
     */
    val sinkTablePath = jobConfig.tablePathFormat.format(jobConfig.database, jobConfig.sinkTable)

    /**
     * 执行sql查询
     */
    val statement: PreparedStatement = conn.prepareStatement(sql)
    val result: ResultSet = statement.executeQuery()

    println("表：" + jobConfig.database + "." + sourceTable + "开始处理")
    val jsonList: ListBuffer[String] = ListBuffer()
    // 1. 拉取数据
    while (result.next()) {
      val map: java.util.HashMap[String, Object] = new util.HashMap()
      sourceTypeMap.map(sourceType => {
        val columnCode: String = sourceType._1
        val columnType: String = sourceType._2.trim
        map.put(columnCode, DataType.getType(columnType))
      })
      val json = new JSONObject(map)
      jsonList.append(json.toJSONString)
    }

    // 2. 存在新数据
    if (jsonList.nonEmpty) {
      import spark.implicits._
      val ds: Dataset[String] = spark.createDataset(jsonList)
      val jdbcDF = spark.read.json(ds)

      // 3. 字段转换
      val viewName = "tmpTable_" + jobConfig.taskID
      jdbcDF.createOrReplaceTempView(viewName)
      val rowsDF: DataFrame = spark.sql(generateSqlJdbc(sourceTypeMappings, sinkTypeMap, viewName)).toDF()

      if (offsetColumns != null) {
        rowsDF.sort(offsetColumns.mkString(",").split(",").map(new Column(_)): _*)
      }

      // 4. 分区处理
      val resultDF = partitionProcess(rowsDF, jobConfig)

      // 5. 入湖
      new FrameWriter(resultDF, jobConfig.writeMode, Option(null)).write(sinkTablePath, Option(jobConfig.upsertKey), Option(null))

      println("表:" + jobConfig.database + "." + sourceTable + "处理结束")
    } else {
      println("没有查询到数据")
    }
  }

  /**
   * 生成类型匹配
   *
   * @param sourceTypeMappings [("C1:int","c2:int")] : 源表字段类型信息
   * @param sinkTypeMap        [(c1->string,c2->string)] : delta表字段类型信息
   * @return [(C1->string,c2->string)]
   */
  def generateSinkTypeMappings(sourceTypeMappings: Array[String], sinkTypeMap: Map[String, String]): Map[String, String] = {
    sourceTypeMappings.map(
      column => {
        val split = column.split(":")
        (split(0), sinkTypeMap(split(1)))
      }
    ).toMap[String, String]
  }

  /**
   * 生成sql语句
   *
   * @param sourceTypeMappings
   * @param sinkTypeMap
   * @param taskID
   * @return
   */
  def generateSqlJdbc(sourceTypeMappings: Array[String], sinkTypeMap: Map[String, String], taskID: String): String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append("select ")
    sourceTypeMappings.map(column => {
      val split = column.split(":")
      val dataType = sinkTypeMap(split(1))
      val extraOperationPrefix = "cast("
      val extraOperationSuffix = " as " + dataType + ")"
      stringBuilder.append(extraOperationPrefix).append("`").append(split(0)).append("`").append(extraOperationSuffix).append(" as ").append(split(1))
    }).mkString(",")
    stringBuilder.append(" from tmpTable_").append(taskID).toString()
  }

  /**
   * 分区字段处理
   *
   * @param dataFrame
   * @param jobConfig
   * @return
   */
  def partitionProcess(dataFrame: DataFrame, jobConfig: HiveToDeltaJobConfig): DataFrame = {
    var resultDF: DataFrame = dataFrame
    val partitioner: TPartition = PartitionFactory.Partition(jobConfig.partitionFormat)
    if (jobConfig.partitionTarget != null) {
      val partition = udf(partitioner.getPartition _)
      resultDF = dataFrame.withColumn(jobConfig.partitionTarget, partition(col(jobConfig.partitionSource).cast("timestamp").cast("bigint"))).toDF()
    }
    resultDF
  }
}
