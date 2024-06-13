package com.nebula.hs.utils

import com.alibaba.fastjson.JSON
import com.hs.config._
import com.hs.etl.Etl
import com.hs.etl.config.source.{DeltaSourceConfig, EsSourceConfig, JdbcSourceConfig, MongoSourceConfig}
import com.hs.integration.{DeltaToJdbc, JdbcToDelta}
//import com.hs.integration.{DeltaToJdbc, EsToDelta, JdbcToDelta, MongoToDelta}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
 * 增量读取时，offset处理
 */
object OffsetUtil {

  /**
   * 增量读取时，更改querySql,拼接增量
   *
   * @param spark   [[SparkSession]]
   * @param jobConf [[JdbcToDeltaJobConfig]]
   * @return [[String]]
   */
  def getQuerySql(spark: SparkSession, jobConf: JdbcToDeltaJobConfig): String = {
    var querySql: String = jobConf.query
    val offsetPathFinal = JdbcToDelta.offsetPath.format(jobConf.taskID)
    val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPathFinal))
    //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal))
    if ("incr".equals(jobConf.readMode) && offsetPathExists
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).nonEmpty
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).length == 2) {
      val offsetJson = spark.read.option("header", value = true).csv(offsetPathFinal).toJSON.collectAsList().get(0)
      if (JSON.parseObject(offsetJson).get("offset") != null) {
        val offsetValue = JSON.parseObject(offsetJson).get("offset").toString
        val offsetColumns = if (jobConf.jdbcUrl.contains("oracle")) "\"" + jobConf.offsetColumns + "\"" else jobConf.offsetColumns
        //字段和类型map
        val colNameAndType = jobConf.sourceColumns.split(",").map(col => {
          col.split(":")(0) -> col.split(":")(1)
        }).toMap
        //获取offset字段类型
        val offseType = colNameAndType.get(jobConf.offsetColumns).toString.toLowerCase
        if (jobConf.jdbcUrl.contains("oracle") &&
          (offseType.contains("timestamp") || offseType.contains("date"))
        ) {
          querySql = s"select * from (${jobConf.query}) temp_${jobConf.taskID} where $offsetColumns >= to_timestamp('${offsetValue}','yyyy-mm-dd hh24:mi:ss.ff') "
        } else if (jobConf.jdbcUrl.contains("hive") &&
          (offseType.contains("timestamp") || offseType.contains("date"))
        ) {
          querySql = s"""select * from (${jobConf.query}) temp_${jobConf.taskID} where unix_timestamp($offsetColumns, \"yyyy-MM-dd'T'HH:mm:ss\") >= unix_timestamp('${offsetValue}', \"yyyy-MM-dd'T'HH:mm:ss\") """
        } else {
          querySql = s"select * from (${jobConf.query}) temp_${jobConf.taskID} where $offsetColumns >= '${offsetValue}' "
        }
        println("增量查询offset为:" + offsetValue)
      }
    }
    querySql
  }

  /**
   * 增量读取时，更改querySql,拼接增量
   *
   * @param spark   [[SparkSession]]
   * @param jobConf [[MongoToDeltaJobConfig]]
   * @return [[String]]
   */
  //  def getMongoQuerySql(spark: SparkSession, jobConf: MongoToDeltaJobConfig): String = {
  //    var querySql: String = jobConf.sql
  //    val offsetValue: String = try {
  //      val offsetPathFinal = MongoToDelta.offsetPath.format(jobConf.taskID)
  //      val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPathFinal))
  //      //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal))
  //      if ("incr".equals(jobConf.readMode) && offsetPathExists
  //        && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).nonEmpty
  //        && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).length == 2) {
  //        val offsetJson = spark.read.option("header", value = true).csv(offsetPathFinal).toJSON.collectAsList().get(0)
  //        JSON.parseObject(offsetJson).get("offset").toString
  //      } else {
  //        "2018-01-01 00:00:00"
  //      }
  //    } catch {
  //      case e: Exception => {
  //        e.printStackTrace()
  //        "2018-01-01 00:00:00"
  //      }
  //    }
  //    if ("incr".equals(jobConf.readMode)) {
  //      querySql = s"select * from (${jobConf.sql}) ${jobConf.taskID}_temp where ${jobConf.offsetColumns} >= '${offsetValue}' "
  //    }
  //    querySql
  //  }

  /**
   * 增量读取时，更改querySql,拼接增量
   *
   * @param spark   [[SparkSession]]
   * @param jobConf [[JdbcToDeltaJobConfig]]
   * @return [[String]]
   */
  //  def getEsQuerySql(spark: SparkSession, jobConf: EsToDeltaJobConfig): String = {
  //    var querySql: String = "select * from temp"
  //    val offsetPathFinal = EsToDelta.offsetPath.format(jobConf.taskID)
  //    val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPathFinal))
  //    //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal))
  //    if ("incr".equals(jobConf.readMode) && offsetPathExists
  //      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).nonEmpty
  //      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).length == 2) {
  //      val offsetJson = spark.read.option("header", value = true).csv(offsetPathFinal).toJSON.collectAsList().get(0)
  //      val offsetValue = JSON.parseObject(offsetJson).get("offset").toString
  //      querySql = s"select * from temp where ${jobConf.offsetColumns}>=${offsetValue}"
  //      println("增量查询offset为:" + offsetValue)
  //    }
  //    querySql
  //  }


  /**
   *
   * @param spark
   * @param deltaToJdbcConfig
   * @return
   */
  def getDeltaQuerySql(spark: SparkSession, deltaToJdbcConfig: DeltaToJdbcConfigV2): String = {
    var sql = deltaToJdbcConfig.query
    val deltaOffsetPath = DeltaToJdbc.offsetPath.format(deltaToJdbcConfig.taskID)
    val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(deltaOffsetPath))
    //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(deltaOffsetPath))
    //todo 库名表名修改替换
    val oldTable = deltaToJdbcConfig.sourceDatabase + "." + deltaToJdbcConfig.sourceTable
    val newTable = "tmp_" + deltaToJdbcConfig.taskID

    if ("incr".equals(deltaToJdbcConfig.readMode) && offsetPathExists
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(deltaOffsetPath)).nonEmpty
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(deltaOffsetPath)).length == 2) {
      val offsetJson = spark.read.option("header", value = true).csv(deltaOffsetPath).toJSON.collectAsList().get(0)
      if (JSON.parseObject(offsetJson).get("offset") != null) {
        val offsetValue = JSON.parseObject(offsetJson).get("offset").toString
        sql = s"select * from (${deltaToJdbcConfig.query.replace(oldTable, newTable)}) ${deltaToJdbcConfig.taskID}_temp where ${deltaToJdbcConfig.offsetColumns} > '${offsetValue}' "
        println("增量查询offset为:" + offsetValue)
      } else {
        sql = sql.replace(oldTable, newTable)
      }
    } else {
      sql = sql.replace(oldTable, newTable)
    }
    sql
  }

  def getJdbcQuerySql(spark: SparkSession, jobConf: JdbcToJdbcJobConfig): String = {
    var querySql: String = jobConf.query
    val offsetPathFinal = JdbcToDelta.offsetPath.format(jobConf.taskID)
    val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPathFinal))
    if ("incr".equals(jobConf.readMode) && offsetPathExists
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).nonEmpty
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).length == 2) {
      val offsetJson = spark.read.option("header", value = true).csv(offsetPathFinal).toJSON.collectAsList().get(0)
      if (JSON.parseObject(offsetJson).get("offset") != null) {
        val offsetValue = JSON.parseObject(offsetJson).get("offset").toString
        val offsetColumns = if (jobConf.sourceJdbcUrl.contains("oracle")) "\"" + jobConf.offsetColumns + "\"" else jobConf.offsetColumns
        //字段和类型map
        val colNameAndType = jobConf.sourceColumns.split(",").map(col => {
          col.split(":")(0) -> col.split(":")(1)
        }).toMap
        //获取offset字段类型
        val offseType = colNameAndType.get(jobConf.offsetColumns).toString.toLowerCase
        if (jobConf.sourceJdbcUrl.contains("oracle") &&
          (offseType.contains("timestamp") || offseType.contains("date"))
        ) {
          querySql = s"select * from (${jobConf.query}) temp_${jobConf.taskID} where $offsetColumns >= to_timestamp('${offsetValue}','yyyy-mm-dd hh24:mi:ss.ff') "
        } else {
          querySql = s"select * from (${jobConf.query}) temp_${jobConf.taskID} where $offsetColumns >= '${offsetValue}' "
        }
        println("增量查询offset为:" + offsetValue)
      }
    }
    querySql
  }

  def getHiveQuerySql(spark: SparkSession, jobConf: JdbcToHiveJobConfig): String = {
    var querySql: String = jobConf.query
    val offsetPathFinal = JdbcToDelta.offsetPath.format(jobConf.taskID)
    val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPathFinal))
    if ("incr".equals(jobConf.readMode) && offsetPathExists) {
      val offsetJson = spark.read.option("header", value = true).csv(offsetPathFinal).toJSON.collectAsList().get(0)
      if (JSON.parseObject(offsetJson).get("offset") != null) {
        val offsetValue = JSON.parseObject(offsetJson).get("offset").toString
        val offsetColumns = if (jobConf.sourceJdbcUrl.contains("oracle")) "\"" + jobConf.offsetColumns + "\"" else jobConf.offsetColumns
        //字段和类型map
        val colNameAndType = jobConf.sourceColumns.split(",").map(col => {
          col.split(":")(0) -> col.split(":")(1)
        }).toMap
        //获取offset字段类型
        val offseType = colNameAndType.get(jobConf.offsetColumns).toString.toLowerCase
        if (jobConf.sourceJdbcUrl.contains("oracle") &&
          (offseType.contains("timestamp") || offseType.contains("date"))
        ) {
          querySql = s"select * from (${jobConf.query}) temp_${jobConf.taskID} where $offsetColumns >= to_timestamp('${offsetValue}','yyyy-mm-dd hh24:mi:ss.ff') "
        } else {
          querySql = s"select * from (${jobConf.query}) temp_${jobConf.taskID} where $offsetColumns >= '${offsetValue}' "
        }
        println("增量查询offset为:" + offsetValue)
      }
    }
    querySql
  }


  def getQuerySqlByJdbcConfig(spark: SparkSession, jobConf: JdbcSourceConfig): String = {
    var querySql: String = jobConf.getQuery
    val offsetPathFinal = Etl.offsetPath.format(jobConf.getTaskID)
    val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPathFinal))
    //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal))
    if ("incr".equals(jobConf.getReadMode) && offsetPathExists
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).nonEmpty
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).length == 2) {
      val offsetJson = spark.read.option("header", value = true).csv(offsetPathFinal).toJSON.collectAsList().get(0)
      if (JSON.parseObject(offsetJson).get("offset") != null) {
        val offsetValue = JSON.parseObject(offsetJson).get("offset").toString
        val offsetColumns = if (jobConf.getJdbcUrl.contains("oracle")) "\"" + jobConf.getOffsetColumns + "\"" else jobConf.getOffsetColumns
        //字段和类型map
        val colNameAndType = jobConf.getSourceColumns.split(",").map(col => {
          col.split(":")(0) -> col.split(":")(1)
        }).toMap
        //获取offset字段类型
        val offseType = colNameAndType.get(jobConf.getOffsetColumns).toString.toLowerCase
        if (jobConf.getJdbcUrl.contains("oracle") &&
          (offseType.contains("timestamp") || offseType.contains("date"))
        ) {
          querySql = s"select * from (${jobConf.getQuery}) temp_${jobConf.getTaskID} where $offsetColumns >= to_timestamp('${offsetValue}','yyyy-mm-dd hh24:mi:ss.ff') "
        } else if (jobConf.getJdbcUrl.contains("hive") &&
          (offseType.contains("timestamp") || offseType.contains("date"))
        ) {
          querySql = s"""select * from (${jobConf.getQuery}) temp_${jobConf.getTaskID} where unix_timestamp($offsetColumns, \"yyyy-MM-dd'T'HH:mm:ss\") >= unix_timestamp('${offsetValue}', \"yyyy-MM-dd'T'HH:mm:ss\") """
        } else {
          querySql = s"select * from (${jobConf.getQuery}) temp_${jobConf.getTaskID} where $offsetColumns >= '${offsetValue}' "
        }
        println("增量查询offset为:" + offsetValue)
      }
    }
    querySql
  }

  def getQuerySqlByEsSourceConfig(spark: SparkSession, jobConf: EsSourceConfig): String = {
    var querySql: String = "select * from temp"
    val offsetPathFinal = Etl.offsetPath.format(jobConf.getTaskID)
    val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPathFinal))
    //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal))
    if ("incr".equals(jobConf.getReadMode) && offsetPathExists
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).nonEmpty
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).length == 2) {
      val offsetJson = spark.read.option("header", value = true).csv(offsetPathFinal).toJSON.collectAsList().get(0)
      val offsetValue = JSON.parseObject(offsetJson).get("offset").toString
      querySql = s"select * from temp where ${jobConf.getOffsetColumns}>=${offsetValue}"
      println("增量查询offset为:" + offsetValue)
    }
    querySql
  }

  def getQuerySqlByMongoSourceConfig(spark: SparkSession, jobConf: MongoSourceConfig): String = {
    var querySql: String = jobConf.getSql
    val offsetValue: String = try {
      val offsetPathFinal = Etl.offsetPath.format(jobConf.getTaskID)
      val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPathFinal))
      //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal))
      if ("incr".equals(jobConf.getReadMode) && offsetPathExists
        && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).nonEmpty
        && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPathFinal)).length == 2) {
        val offsetJson = spark.read.option("header", value = true).csv(offsetPathFinal).toJSON.collectAsList().get(0)
        JSON.parseObject(offsetJson).get("offset").toString
      } else {
        "2018-01-01 00:00:00"
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        "2018-01-01 00:00:00"
      }
    }
    if ("incr".equals(jobConf.getReadMode)) {
      querySql = s"select * from (${jobConf.getSql}) ${jobConf.getTaskID}_temp where ${jobConf.getOffsetColumns} >= '${offsetValue}' "
    }
    querySql
  }

  def getDeltaQuerySqlByDeltaSourceConfig(spark: SparkSession, deltaToJdbcConfig: DeltaSourceConfig): String = {
    var sql = deltaToJdbcConfig.getQuery
    val deltaOffsetPath = Etl.offsetPath.format(deltaToJdbcConfig.getTaskID)
    val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(deltaOffsetPath))
    //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(deltaOffsetPath))
    //todo 库名表名修改替换
    val oldTable = deltaToJdbcConfig.getSourceDatabase + "." + deltaToJdbcConfig.getSourceTable
    val newTable = "tmp_" + deltaToJdbcConfig.getTaskID

    if ("incr".equals(deltaToJdbcConfig.getReadMode) && offsetPathExists
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(deltaOffsetPath)).nonEmpty
      && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(deltaOffsetPath)).length == 2) {
      val offsetJson = spark.read.option("header", value = true).csv(deltaOffsetPath).toJSON.collectAsList().get(0)
      if (JSON.parseObject(offsetJson).get("offset") != null) {
        val offsetValue = JSON.parseObject(offsetJson).get("offset").toString
        sql = s"select * from (${deltaToJdbcConfig.getQuery.replace(oldTable, newTable)}) ${deltaToJdbcConfig.getTaskID}_temp where ${deltaToJdbcConfig.getOffsetColumns} > '${offsetValue}' "
        println("增量查询offset为:" + offsetValue)
      } else {
        sql = sql.replace(oldTable, newTable)
      }
    } else {
      sql = sql.replace(oldTable, newTable)
    }
    sql
  }
}
