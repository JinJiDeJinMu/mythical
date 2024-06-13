package com.nebula.hs

import com.alibaba.fastjson.JSON
import com.hs.config.DeltaMulTableConfig
import com.hs.utils.DeltaMergeUtil.orderExpr
import com.hs.utils.{FrameWriter, JdbcWriter, Supplement}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.collection.mutable

/**
 * Delta 表数据处理
 * {{{
 *       1.支持单表或多表
 *       2.支持计算方式：
 *          全量:不做处理
 *          增量:增量表存储任务结束后的 offset
 *              ··支持自定义增量字段
 *              ··支持不同表不同增量字段
 *       3.offset存储：自动排除不需要存储 offset 的全量表
 * }}}
 * == Example ==
 *
 * {{{
 *   import com.hs.DeltaMulTableJob
 *   import com.hs.config.DeltaMulTableConfig
 *
 *   val jobConf = DeltaMulTableConfig(...)
 *   val job = DeltaMulTableJob(jobConf)
 *   var sql = if (jobConf.sql=""){
 *       """
 *         |
 *         |with t0 as (
 *         |    select id,trade_currency,trade_value
 *         |    from stg_fina_merge_event_di t
 *         |    where is_delete='0'
 *         |        --and date_format(update_time, 'yyyy-MM-dd')=current_date()
 *         |)
 *         |,t1 as (
 *         |    select
 *         |        currency,
 *         |        trade_date,
 *         |        rmb_central_parity
 *         |    from (
 *         |        SELECT
 *         |            currency, trade_date, rmb_central_parity, is_delete,
 *         |            row_number() over(PARTITION BY currency ORDER BY trade_date desc) rn
 *         |        from dim_stds_exchange_rate
 *         |        WHERE is_delete = '0'
 *         |            -- and date_format(update_time, 'yyyy-MM-dd')=current_date()
 *         |    ) t WHERE t.rn = 1
 *         |)
 *         |
 *         |select t0.id
 *         |       ,CASE t0.trade_currency
 *         |            WHEN 'CNY' THEN round(t0.trade_value * 1000000,4)
 *         |            ELSE  round(t0.trade_value * 1000000 * t1.rmb_central_parity,4)
 *         |            END AS trade_value_cal
 *         |from  t0
 *         |left join t1
 *         |on t0.trade_currency = t1.currency
 *         |
 *         |""".stripMargin)
 *   }else{
 *     jobConf.sql
 *   }
 *   val resultDf = job.calculation(sql)
 *
 *   job.saveToDelta(resultDf)
 * }}}
 *
 * @param jobConf [[DeltaMulTableConfig]]
 * @author wzw
 */

case class DeltaMulTableJob(jobConf: DeltaMulTableConfig) {

  /**
   * Delta SparkSession
   */
  val spark: SparkSession = {
    SparkSession
      .builder()
      .config(new SparkConf().setAll(sparkConfMap))
      //.enableHiveSupport()
      .getOrCreate()
  }
  /**
   * DeltaTable 路径格式
   */
  private val tablePathFormat: String = jobConf.tablePathFormat
  /**
   * Offset 路径格式
   */
  private val offsetPathFormat: String = jobConf.offsetPathFormat
  /**
   * Source 端库与表映射关系 {database:table}
   */
  private val sourceDbTableMap: String = jobConf.sourceDbTableMap
  /**
   * Sink 端库表参数映射 {database:table}
   */
  private val sinkDbTableMap: String = jobConf.sinkDbTableMap
  /**
   * Source表DataFrame映射Map
   */
  private val sourceTableDFMap: mutable.Map[String, DataFrame] = mutable.Map[String, DataFrame]()
  /**
   * Source表Offset映射Map
   */
  private val sourceTableOffsetMap: mutable.Map[String, String] = mutable.Map[String, String]()

  /**
   * Source表DataFrame映射Map
   */
  //val sourceDfMap: mutable.Map[String, DataFrame] = mutable.Map[String, DataFrame]()
  /**
   * Source端增量表当前任务Offset映射Map
   */
  private val latestTableOffsetDfMap: mutable.Map[String, DataFrame] = mutable.Map[String, DataFrame]()

  /**
   * Spark Application name
   */
  private val appName: String = jobConf.appName

  /**
   * 计算方式
   */
  private val calculationMode: String = jobConf.calculationMode

  /**
   * 数据写入目标表方式
   */
  private val writeMode: String = jobConf.writeMode

  /**
   * Source表增量字段
   */
  private val incrColumn: String = jobConf.incrColumn

  /**
   * Merge 条件字段
   */
  private val mergeKeys: String = jobConf.mergeKeys

  /**
   * Merge 前,数据去重排序字段
   */
  private val sortColumns: String = jobConf.sortColumns

  /**
   * 业务sql
   */
  private val sql: String = jobConf.sql

  private val jdbcWriteOptions = Map(
    "url" -> jobConf.jdbcUrl,
    "user" -> jobConf.username,
    "password" -> jobConf.password,
    "batchsize" -> jobConf.batchSize,
    "dbtable" -> s"${jobConf.targetDatabase}.${jobConf.targetTable}"
  )

  /**
   * kafkaServers
   */
  private val kafkaServers: String = jobConf.kafkaServers

  /**
   * topic
   */
  private val topic: String = jobConf.topic
  /**
   * Spark session 配置信息
   */
  private val sparkConfMap = Map(
    "spark.app.name" -> appName,
    //"spark.master" -> "local[*]",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite" -> "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED"
  )

  /**
   * 设置Session级 HADOOP_USER_NAME
   */
  System.setProperty("HADOOP_USER_NAME", "hdfs")
  /**
   * Source表于增量字段Map
   */
  private var sourceTableIncrColumnMap: mutable.Map[String, String] = mutable.Map[String, String]()
  private var stringBuilder = new StringBuilder("{")

  /**
   * sql 统一出口
   *
   * /*@param sql [[String]] 原始sql*/
   *
   * @return 可执行sql
   */
  def generateStandardSql(): String = {
    val fullJobOffsetValue = getFullJobOffsetValue(getFullJobOffsetPath(sourceDbTableMap))
    val newSql = if (sql != null) {
      if (calculationMode.equals("full") && sql.contains("#{last_time}")) {
        sql.replace("#{last_time}"
          , if (fullJobOffsetValue != null) {
            "'" + fullJobOffsetValue + "'"
          } else {
            "'" + "2018-01-01 00:00:00" + "'"
          }
        )
      } else {
        jobConf.sql
      }
      /* No need! Unnecessary action! */
      //rewriteSql(jobConf.sql,jobConf.columnMap)
    } else {
      //兼容单表模式下仅传入字段映射的情况(非sql传入)
      generateSql(jobConf.columnMap)
    }
    println("执行sql：" + newSql)
    newSql
  }

  /**
   * sql拼接 针对单表输出,仅传入source端和sink端的字段映射时
   *
   * @param columnMap [[String]]
   * @return sql [[String]]
   */
  private def generateSql(columnMap: String): String = {
    val stringBuilder = new StringBuilder("select ")
    val str = columnMap.split(",").map(col => {
      col.split(":")(0) + " as " + col.split(":")(1)
    }).mkString(",")
    stringBuilder.append(str)
      .append(" from ")
      .append(
        sourceDbTableMap.split(",")(0).split(":")(1)
      ).toString()
  }

  /**
   * Function: Full quantity/increment computer and join
   *
   * @param sql [[String]]
   * @return [[DataFrame]]
   */
  def calculation(sql: String): DataFrame = {
    val sourceDfMap: mutable.Map[String, DataFrame] = createDataFrame()
    calculationMode match {
      case "full" => {
        sourceDfMap.foreach(kv => {
          kv._2.createOrReplaceTempView(kv._1)
        })
        spark.sql(sql)
      }
      case "incr" => {
        // (tableName -> offsetValue) 信息
        val sourceTableOffsetMap: mutable.Map[String, String] = getSourceTableOffsetMap()
        println(s"增量表信息:${sourceTableOffsetMap}")
        sourceDfMap.foreach(kv => {
          // 增量表 不同表的增量字段不同
          if (sourceTableOffsetMap.contains(kv._1)) {
            var incrDF: DataFrame = kv._2
            if (sourceTableOffsetMap(kv._1) != null) {
              //val incrDF: DataFrame = kv._2.where(s"${incrColumn}>='${sourceTableOffsetMap(kv._1)}'")
              incrDF = incrDF.where(s"${sourceTableIncrColumnMap(kv._1)}>'${sourceTableOffsetMap(kv._1)}'")
            }
            incrDF.createOrReplaceTempView(kv._1)
            incrDF.cache()
            //spark.table(kv._1).cache()
            //var latestTableOffsetDfMap: mutable.Map[String, DataFrame] = mutable.Map[String, DataFrame]()
            if (!incrDF.isEmpty) {
              latestTableOffsetDfMap += (
                //kv._1 -> incrDF.selectExpr(s"cast(max(${incrColumn})  as string) as offset ")
                kv._1 -> incrDF.selectExpr(s"cast(max(${sourceTableIncrColumnMap(kv._1)})  as string) as offset ")
                )
              //saveOffset(latestTableOffsetDfMap)
            }
          } else {
            // 全量表
            kv._2.createOrReplaceTempView(kv._1)
          }
        })
        spark.sql(sql)
      }
      case _ => throw new IllegalArgumentException("不支持[%s]读取模式".format(calculationMode))
    }
  }

  /**
   * 增量读获 deltaTable 任务的 offset 获取
   *
   * @return sourceTableOffsetMap [[mutable.Map[String, String]]]
   *         eg.{(tableName->offsetValue,tableName1->offsetValue,tableName2->offsetValue)}
   */
  private def getSourceTableOffsetMap(): mutable.Map[String, String] = {
    // (tableName -> offsetPath) 信息
    val sourceTableOffsetMap: mutable.Map[String, String] = generateDeltaTableOffsetPath(sourceDbTableMap)
    // (tableName -> offsetValue) 信息
    val sourceTableOffsetValueMap: mutable.Map[String, String] = mutable.Map[String, String]()
    sourceTableOffsetMap.foreach(kv => {
      //var offsetValue: String = "2018-01-01 00:00:00"
      val sourceTableName = kv._1
      val sourceTableOffsetPath = kv._2
      val offsetValue: String = try {
        val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(sourceTableOffsetPath))
        //val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(sourceTableOffsetPath))
        if (offsetPathExists && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(sourceTableOffsetPath)).nonEmpty
          && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(sourceTableOffsetPath)).length == 2) {
          val offsetJson: String = spark.read.option("header", value = true).csv(sourceTableOffsetPath).toJSON.collectAsList().get(0)
          JSON.parseObject(offsetJson).get("offset").toString
          //sourceTableOffsetValueMap += (sourceTableName -> offsetValue)
        } else (
          null
          )
      } catch {
        case e: Exception => {
          e.printStackTrace()
          null
        }
      }
      sourceTableOffsetValueMap += (sourceTableName -> offsetValue)
    })
    println(s"tableName:offsetValue = ${sourceTableOffsetValueMap}")
    sourceTableOffsetValueMap
  }

  /**
   * SparkSql 读取指定路径的 deltaTable
   *
   * @return sourceTableDFMap [[mutable.Map[String, DataFrame]]]
   *         eg.{(tableName->DataFrame,tableName1->DataFrame,tableName2->DataFrame)}
   */
  private def createDataFrame(): mutable.Map[String, DataFrame] = {
    val sourceTablePathMap: mutable.Map[String, String] = generateDeltaTablePath(sourceDbTableMap)
    sourceTablePathMap.foreach(kv => {
      val sourceTableName = kv._1
      val sourceDeltaTable = kv._2
      sourceTableDFMap += (sourceTableName -> spark.read.format("delta").load(sourceDeltaTable))
    })
    sourceTableDFMap
  }

  /**
   * 生成 delta 表的有效路径
   * eg.{(t->tPath,t1->t1Path,t2->t2Path)}
   *
   * @param dbTableMap [[String]]
   *                   eg.{ods:t1,ods:t2,dim:t3}
   * @return tarTablePathMap [[mutable.Map[String, String]]
   *         eg. {(tableName -> tablePath),(table1Name -> table1Path)}
   */
  private def generateDeltaTablePath(dbTableMap: String): mutable.Map[String, String] = {
    val tarTablePathMap: mutable.Map[String, String] = mutable.Map[String, String]()
    dbTableMap.split(",").foreach(source => {
      val split = source.split(":")
      tarTablePathMap += (split(1) -> tablePathFormat.format(split(0), split(1)))
    })
    tarTablePathMap
  }

  /**
   * 内置业务补充
   *
   * @param dataFrame [[DataFrame]]
   * @return newDataFrame
   */
  def dataFrameTransform(dataFrame: DataFrame): DataFrame = {
    var resultDf = dataFrame

    // 00. full job incr filter
    val fullJobOffsetValue = getFullJobOffsetValue(getFullJobOffsetPath(sourceDbTableMap))
    resultDf = if (calculationMode.equals("full")
      && (!sql.contains("#{last_time}"))
      && jobConf.fullJobOffsetCol != null && jobConf.fullJobOffsetCol != ""
      && fullJobOffsetValue != null) {
      resultDf.where(s"${jobConf.fullJobOffsetCol} > '${fullJobOffsetValue}'")
    } else {
      resultDf
    }

    // 01. add watermark
    if (jobConf.isWatermark.equals("1")) {
      resultDf = Supplement.traceData(jobConf.taskID, resultDf)
    }

    resultDf
  }

  /**
   * 获取全量任务下的 job 级的offset值
   *
   * @param offsetPath
   * @return
   */
  private def getFullJobOffsetValue(offsetPath: String): String = {
    val offsetValue: String = try {
      val offsetPathExists = FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(offsetPath))
      if (offsetPathExists && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPath)).nonEmpty
        && FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(offsetPath)).length == 2) {
        val offsetJson: String = spark.read.option("header", value = true).csv(offsetPath).toJSON.collectAsList().get(0)
        JSON.parseObject(offsetJson).get("offset").toString
      } else (
        null
        )
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
    offsetValue
  }

  /**
   * 根据写入方式，将输入的 DataFrame 写至目标表
   * 支持写入多个目标表
   *
   * @param dataFrame [[DataFrame]]
   */
  def saveToDelta(dataFrame: DataFrame): Unit = {
    val writer = FrameWriter(dataFrame, writeMode.toLowerCase, Option(null))
    val sinkTablePathMap: mutable.Map[String, String] = generateDeltaTablePath(sinkDbTableMap)
    sinkTablePathMap.foreach(kv => {
      val sinkDeltaTable = kv._2
      writer.write(sinkDeltaTable, Option(mergeKeys), Option(orderExpr(sortColumns)))
    })
    println(writeMode + "数据完成！")
    // 增量计算模式下 写入表成功后存储增量offset
    if (calculationMode.equals("incr") && latestTableOffsetDfMap.nonEmpty) {
      saveOffset(latestTableOffsetDfMap)
    }
  }

  /**
   * 根据写入方式，将输入的 DataFrame 写至目标表(数据库表)
   *
   * @param dataFrame [[DataFrame]]
   */
  def saveToJdbc(dataFrame: DataFrame): Unit = {
    val writer = new JdbcWriter(dataFrame, writeMode.toLowerCase, jdbcWriteOptions)
    writer.write(mergeKeys)
    println(writeMode + "数据完成！")
    // 增量计算模式下 写入表成功后存储增量offset
    if (calculationMode.equals("incr") && latestTableOffsetDfMap.nonEmpty) {
      saveOffset(latestTableOffsetDfMap)
    }
    // 全量模式下 写入表成功后存储job的增量offset
    if (calculationMode.equals("full") &&
      (jobConf.sql.contains("#{last_time}") || (jobConf.fullJobOffsetCol != null && jobConf.fullJobOffsetCol != "")) && !dataFrame.isEmpty) {
      saveOffset(dataFrame, getFullJobOffsetPath(sourceDbTableMap))
    }
    val syncTime: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    // 统计信息发送kafka
    if (jobConf.kafkaServers != null && jobConf.topic != null) {
      stringBuilder.append(
        "\"id\":\"%s\"".format(jobConf.taskID)
          + ","
          + "\"dataNum\":%d".format(dataFrame.count())
          + ","
          + "\"apiDataTypeEnum\":\"%s\"".format("CHANNEL_TYPE_OFFLINE_3")
          + ","
          + "\"syncTime\":\"%s\"".format(syncTime)
          + "}"
      )
      println("输出：" + stringBuilder.toString)
      sendToKafka(stringBuilder.toString)
    }
  }

  /**
   * 向kafka发送消息
   *
   * @param message [[String]]
   */
  private def sendToKafka(message: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)
    println("发送成功！")
    producer.close()
  }

  /**
   * 获取全量任务下的 job 级的offset值存储的路径
   *
   * @param dbTableMap
   * @return
   */
  private def getFullJobOffsetPath(dbTableMap: String): String = {
    val sourceDb: String = dbTableMap.split(",")(0).split(":")(0)
    val tableLink = new StringBuilder(s"${jobConf.taskID}")
    dbTableMap.split(",").foreach(source => {
      tableLink.append(source.split(":")(1))
    })
    offsetPathFormat.format(sourceDb, jobConf.targetDatabase, tableLink, jobConf.targetTable)
  }

  /**
   * 存储offset
   *
   * @param tableOffsetDfMap [[mutable.Map[String, String]]]
   */
  private def saveOffset(tableOffsetDfMap: mutable.Map[String, DataFrame]): Unit = {
    val sourceTableOffsetMap: mutable.Map[String, String] = generateDeltaTableOffsetPath(sourceDbTableMap)
    println(s"存储offset的表:${sourceTableOffsetMap}")
    sourceTableOffsetMap.foreach(kv => {
      // 有 offset 值再存储
      if (tableOffsetDfMap.contains(kv._1) && !tableOffsetDfMap(kv._1).isEmpty) {
        tableOffsetDfMap(kv._1).write.option("header", value = true).mode(SaveMode.Overwrite).csv(kv._2)
        println(kv._1 + ": offset存储成功！ offset值: " + kv._2)
      } else {
        println(kv._1 + " 无增量数据")
      }
    })
  }

  /**
   * 生成存储读取 delta 表任务 offset 的有效路径 排除全量读取的表
   *
   * @param dbTableMap [[String]]
   *                   eg.{ods:t1:incr,ods:t2:incr,dim:t3:full}
   * @return sourceTableOffsetPathMap [[mutable.Map[String, String]]
   *         eg.{(tableName->tOffsetPath,tableName1->t1OffsetPath,tableName2->t2OffsetPath)}
   */
  private def generateDeltaTableOffsetPath(dbTableMap: String): mutable.Map[String, String] = {
    var sourceTableOffsetMap: mutable.Map[String, String] = mutable.Map[String, String]()
    val sourceExcludeTableMap: mutable.Map[String, String] = mutable.Map[String, String]()
    // FIXME:
    val sourceDb: String = dbTableMap.split(",")(0).split(":")(0)
    var sinkDb: String = ""
    sourceDb match {
      case "stg" => sinkDb = "ods"
      case "ods" => sinkDb = "dwd"
      case "dwd" => sinkDb = "dws"
      case "dws" => sinkDb = "ads"
      case _ => throw new IllegalArgumentException("不支持[%s]分层".format(sourceDb))
    }
    //println(s"${sourceDb} -> ${sinkDb} 任务")
    println(s"${sourceDb} -> ${jobConf.targetDatabase} 任务")
    //offset任务sink端  FIXME:sink多个表时处理
    /*
        val sinkTable = sinkDbTableMap.split(",").map(x => {
          x.split(":")(1)
        })
    */
    dbTableMap.split(",").foreach(source => {
      val split = source.split(":")
      sourceTableOffsetMap += (split(1) -> offsetPathFormat.format(sourceDb, jobConf.targetDatabase, split(1), jobConf.targetTable)) //sinkTable(0)
      sourceExcludeTableMap += (split(1) -> split(2))
      /*表->增量字段*/
      sourceTableIncrColumnMap += (
        split(1) -> {
          if (split.length < 4) {
            "insert_time"
          } else {
            split(3)
          }
        })
    })
    println(s"全部:${sourceTableOffsetMap}  排除信息:${sourceExcludeTableMap}")
    sourceTableOffsetMap = sourceFullTableExclude(sourceTableOffsetMap, sourceExcludeTableMap)
    sourceTableIncrColumnMap = sourceFullTableExclude(sourceTableIncrColumnMap, sourceExcludeTableMap)
    println(s"实际增量: ${sourceTableOffsetMap}")
    println(s"实例增量表字段: ${sourceTableIncrColumnMap}")
    sourceTableOffsetMap
  }

  /**
   * Map中排除全量使用的表,不存储增量offset
   *
   * @param sourceTableOffsetMap  [[mutable.Map[String, String]]]
   *                              eg.{t1:offsetPath,t2:offsetPath,t3:offsetPath}
   * @param sourceExcludeTableMap [[mutable.Map[String, String]]]
   *                              eg.{t1:incr,t2:incr,t3:full}
   * @return sourceTableOffsetMap [[mutable.Map[String, String]]]
   */
  private def sourceFullTableExclude(sourceTableOffsetMap: mutable.Map[String, String]
                                     , sourceExcludeTableMap: mutable.Map[String, String]): mutable.Map[String, String] = {
    sourceExcludeTableMap.foreach(kv => {
      if (kv._2 == "full") {
        println(s"全量读取表：${kv._1} 不存储offset! ")
        sourceTableOffsetMap -= kv._1
      }
    })
    println(s"排除后:${sourceTableOffsetMap}")
    sourceTableOffsetMap
  }

  /**
   * 存储offset
   *
   * @param dataFrame
   * @param offsetPath
   */
  private def saveOffset(dataFrame: DataFrame, offsetPath: String): Unit = {
    val offsetCol = if (jobConf.sql.contains("#{last_time}")) {
      if (jobConf.fullJobOffsetCol != null && jobConf.fullJobOffsetCol != "") {
        jobConf.fullJobOffsetCol
      } else {
        "update_time"
      }
    } else {
      jobConf.fullJobOffsetCol
    }
    dataFrame
      .selectExpr(s"cast(max(${offsetCol})  as string) as offset ")
      .write.option("header", value = true).mode(SaveMode.Overwrite).csv(offsetPath)
    println("full Job: job offset存储成功！")
  }

  /**
   * sql根据字段映射重写
   *
   * @param columnMap [[String]]
   * @return sql [[String]]
   */
  private def rewriteSql(sql: String, columnMap: String): String = {
    /* 这种方式应用端传过来的sql中不能as字段,as的作用体现在字段映射中 */
    val stringBuilder = new StringBuilder("select ")
    val str = columnMap.split(",").map(col => {
      col.split(":")(0) + " as " + col.split(":")(1)
    }).mkString(",")
    stringBuilder.append(str)
      .append(" from ( ").append(sql).append(")")
      .toString()
  }
}
