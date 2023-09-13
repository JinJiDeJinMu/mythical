/*
 *@Author   : DoubleTrey
 *@Time     : 2023/2/14 13:57
 */

package com.hs.utils

import cn.hutool.core.lang.UUID
import cn.hutool.crypto.symmetric.AES
import cn.hutool.crypto.{Mode, Padding}
import com.google.gson.Gson
import com.hs.common.{SparkSessionWrapper, SystemField}
import com.hs.config.{DecryptColConfig, EncryptColConfig}
import com.hs.etl.{EncryptDecryptConfig, PartitionsConfig}
import com.hs.utils.CommonFunction.sendToKafka
import com.hs.utils.encrypt.EncryptFactory
import com.hs.utils.partition.{PartitionFactory, TPartition}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}

import java.lang
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.collection.mutable


object Supplement extends SparkSessionWrapper {
  val preUrl: String = "/user/hive/warehouse"

  /**
   * 系统字段补充: insert_time, is_delete
   *
   * @param dataFrame [[DataFrame]]
   * @return
   */
  def systemFieldComplete(dataFrame: DataFrame): DataFrame = {
    var result = dataFrame
    val toComplete = SystemField.values().filter(x => !result.schema.fieldNames.contains(x.name()))
    for (x <- toComplete) {
      x match {
        case SystemField.insert_time => result = result.withColumn("insert_time", current_timestamp())
        case SystemField.is_delete => result = result.withColumn("is_delete", lit(0))
      }
    }
    result = result.withColumn("is_delete", col("is_delete").cast(IntegerType))
      .withColumn("insert_time", col("insert_time").cast(TimestampType))

    result
  }

  /**
   * 根据配置生成分区字段: "二级/三级分区"待扩展
   *
   * @param dataFrame [[DataFrame]]
   * @return
   */
  def partitionProcess(dataFrame: DataFrame): DataFrame = {
    var result = dataFrame
    //for (p <- pConf) {
    //  result = dataFrame.withColumn("", )
    //}
    result
  }

  /**
   * 根据配置生成分区字段: "仅单分区"
   *
   * @param dataFrame       [[DataFrame]]
   * @param partitionType   [[String]]
   * @param partitionSource [[String]]
   * @param partitionTarget [[String]]
   * @param partitionFormat [[String]]
   * @return [[DataFrame]]
   */
  def partitionTrans(dataFrame: DataFrame
                     , partitionType: String
                     , partitionSource: String
                     , partitionTarget: String
                     , partitionFormat: String): DataFrame = {
    if (partitionSource != "" && partitionSource != null && partitionSource.nonEmpty) {
      val partitioner: TPartition = PartitionFactory.Partition(partitionType, partitionFormat)
      dataFrame
        .withColumns(partitioner.getPartition(partitionSource, partitionTarget))
    } else {
      dataFrame
    }
  }


  /**
   * 根据配置生成分区字段
   * 【支持多分区】
   *
   * @param dataFrame
   * @param partitionsConfig
   * @return
   */
  def partitionMulTrans(dataFrame: DataFrame, partitionsConfig: List[PartitionsConfig]): DataFrame = {
    var df = dataFrame
    if (partitionsConfig != null && partitionsConfig.nonEmpty) {
      partitionsConfig.foreach(config => {
        df = if (config.getPartitionSource != "" && config.getPartitionSource != null && config.getPartitionSource.nonEmpty) {
          val partitioner: TPartition = PartitionFactory.Partition(config.getPartitionType, config.getPartitionFormat)
          dataFrame
            .withColumns(partitioner.getPartition(config.getPartitionSource, config.getPartitionTarget))
        } else {
          dataFrame
        }
      })
    }
    df
  }

  /**
   * 对敏感字段进行静态脱敏,加密存储
   *
   * @param dataFrame         [[DataFrame]]
   * @param encryptConfigList [[List]].[[EncryptColConfig]]
   * @return
   */
  def encryptTrans(dataFrame: DataFrame
                   , encryptConfigList: List[EncryptColConfig]): DataFrame = {
    var df = dataFrame
    if (encryptConfigList != null && encryptConfigList.nonEmpty) {
      encryptConfigList.foreach(col => {
        df = EncryptFactory.Encrypt(df, col)
      })
      df
    } else {
      df
    }
  }

  /**
   * 对已加密数据进行解密操作,明文使用
   *
   * @param dataFrame         [[DataFrame]]
   * @param decryptConfigList [[List]].[[DecryptColConfig]]
   * @return
   */
  def decryptTrans(dataFrame: DataFrame
                   , decryptConfigList: List[DecryptColConfig]): DataFrame = {
    var df = dataFrame
    if (decryptConfigList != null && decryptConfigList.nonEmpty) {
      decryptConfigList.foreach(col => {
        df = EncryptFactory.Decrypt(df, col)
      })
      df
    } else {
      df
    }
  }

  /**
   * 加解密统一出口 支持多表多字段
   *
   * @param dataFrame                [[DataFrame]]
   * @param encryptDecryptConfigList [[[EncryptDecryptConfig]]
   * @return newDF
   */
  def encryptDecryptTransMul(dataFrame: mutable.Map[String, DataFrame]
                             , encryptOrDecrypt: String
                             , encryptDecryptConfigList: List[EncryptDecryptConfig]): mutable.Map[String, DataFrame] = {
    val df: mutable.Map[String, DataFrame] = dataFrame
    if (encryptDecryptConfigList != null && encryptDecryptConfigList.nonEmpty) {
      encryptDecryptConfigList.foreach(colConfig => {
        if (df.contains(colConfig.getTableName)) {
          df(colConfig.getTableName) = encryptOrDecrypt match {
            case "encrypt" => EncryptFactory.Encrypt(df(colConfig.getTableName), colConfig)
            case "decrypt" => EncryptFactory.Decrypt(df(colConfig.getTableName), colConfig)
            case _ => throw new IllegalArgumentException("[%s]不与【encrypt,decrypt】任何一个匹配".format(encryptOrDecrypt))
          }
        }
      })
      df
    } else {
      df
    }
  }

  /**
   * 接入总览数据量统计统一出口
   * 对外共享delta表数据数据量统计统一出口
   *
   * @param taskID       [[String]]
   * @param sourceType   [[String]]
   * @param cnt          [[Long]]
   * @param kafkaServers [[String]]
   * @param topic        [[String]]
   */
  def dataCount(taskID: String
                , sourceType: String
                , cnt: Long
                , kafkaServers: String
                , topic: String
                , targetDb: String = null): Unit = {
    if (kafkaServers != null && topic != null) {
      val sysDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val messageMap = new java.util.HashMap[String, Object]()
      messageMap.put("taskID", taskID)
      messageMap.put("dataCnt", cnt.asInstanceOf[lang.Long])
      messageMap.put("sourceType", sourceType)
      messageMap.put("sysDate", sysDate)
      /*存储量*/
      if (targetDb != null && targetDb != "") {
        messageMap.put("dataStorage", (storageCount(targetDb) / 1024 / 1024).asInstanceOf[lang.Long]) //MB
      }
      val messageJson: String = new Gson().toJson(messageMap)
      println(messageJson)
      sendToKafka(messageJson, kafkaServers, topic)
    }
  }

  /**
   * 统计指定指定层所有delta表的物理存储量
   *
   * @param targetDb [[String]] eg.{stg}
   * @return 存储量
   */
  def storageCount(targetDb: String): Long = {
    val path = preUrl + s"/${targetDb}.db"
    var totalFileSize: Long = 0
    val filePath: Path = new org.apache.hadoop.fs.Path(path)
    val fileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files: Array[Path] = FileUtil.stat2Paths(fileSystem.listStatus(filePath))
    val subFileName: Array[String] = files.filter(fileSystem.getFileStatus(_).isDirectory()).map(_.getName)

    subFileName.foreach(subFile => {
      val sourcePath = path + "/" + subFile
      fileSystem.listStatus(new Path(sourcePath)).foreach(file => {
        totalFileSize += file.getLen
      })
    })
    totalFileSize
  }

  /** fixme: 待定
   * 对外共享湖仓数据数据量统计统一出口
   *
   * @param taskID       [[String]]
   * @param sourceType   [[String]]
   * @param cnt          [[Long]]
   * @param kafkaServers [[String]]
   * @param topic        [[String]]
   */
  @deprecated
  def dataCountChannel(taskID: String
                       , sourceType: String
                       , cnt: Long
                       , kafkaServers: String
                       , topic: String): Unit = {
    // 统计信息发送kafka
    if (kafkaServers != null && topic != null) {
      val syncTime: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val messageMap = new java.util.HashMap[String, Object]()
      messageMap.put("id", taskID)
      messageMap.put("dataNum", cnt.asInstanceOf[lang.Long])
      messageMap.put("sourceType", sourceType)
      messageMap.put("apiDataTypeEnum", "CHANNEL_TYPE_OFFLINE_3")
      messageMap.put("syncTime", syncTime)
      val messageJson: String = new Gson().toJson(messageMap)
      println(messageJson)
      sendToKafka(messageJson, kafkaServers, topic)
    }
  }

  /**
   *
   * @param taskID
   * @param dataFrame
   * @return
   */
  def traceData(taskID: String, dataFrame: DataFrame): DataFrame = {
    var df = dataFrame
    val count = dataFrame.rdd.count()
    if (count > 0) {
      val randomNum = scala.util.Random.nextInt(count.toInt - 1)

      val time = Instant.now().getEpochSecond
      val internal_trace_code = taskID + "|" + time + "|hsmap"
      val ENCODE_KEY = "ni2FSZxSx9h1xYJE"
      val IV_KEY = "0000000000000000"
      val aes = new AES(Mode.CBC, Padding.ZeroPadding, new SecretKeySpec(ENCODE_KEY.getBytes(), "AES"), new IvParameterSpec(IV_KEY.getBytes()))
      //加密
      import spark.implicits._

      val arr = new mutable.ArrayBuffer[String]()
      for (i <- 0 until count.toInt) {
        if (i == randomNum) {
          arr += (aes.encryptBase64(internal_trace_code))
        } else {
          arr += (aes.encryptBase64(UUID.randomUUID().toString))
        }
      }
      var frame = spark.sparkContext.parallelize(arr).toDF()
      frame = frame.withColumn("hs_internal_trace_code", col("value")).drop(col("value"))

      df = df.withColumn("rad", rand()).withColumn("hs_id", row_number().over(Window.orderBy("rad")))
      frame = frame.withColumn("rad", rand()).withColumn("hs_id", row_number().over(Window.orderBy("rad")))

      var columns: Array[String] = dataFrame.columns
      val b = Array("hs_internal_trace_code")
      columns = columns ++ b

      df = df.join(
        frame
        , Seq("hs_id")
        , "inner"
      ).selectExpr(columns: _*)
    }
    df.printSchema()
    df
  }
}
