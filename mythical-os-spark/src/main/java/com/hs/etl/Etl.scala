package com.hs.etl

import cn.hutool.core.codec.Base64
import com.google.gson.reflect.TypeToken
import com.google.gson.{GsonBuilder, JsonElement}
import com.hs.common.SparkCommon
import com.hs.etl.config.EtlConfig
import com.hs.etl.sink.EtlSink
import com.hs.etl.source.EtlSource
import org.apache.spark.sql.{Dataset, Row}

import java.util
import scala.collection.mutable.ListBuffer

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
object Etl {
  val offsetPath = "/delta/_offset/etl/%s"
  val jobType = "etl"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    var params = args(0)

    params = Base64.decodeStr(params, "UTF-8")

    println("任务配置信息：" + params)


    val config = EtlConfig.getEtlConfig(params)

    val sourceConfig = config.getSource.getConfig.getAsJsonObject
    val sinkConfig = config.getSink.getConfig.getAsJsonObject

    //hive sink时将用户设置为root,需要在SparkSession创建前设置
    if (config.getSink.getType.equalsIgnoreCase("hive")) {
      System.setProperty("HADOOP_USER_NAME", "root")
    }

    val configJsonObject = EtlConfig.getJsonElement(params).getAsJsonObject

    configJsonObject.entrySet().forEach(config => {
      if (!config.getKey.equalsIgnoreCase("source") && !config.getKey.equalsIgnoreCase("sink")) {
        sourceConfig.add(config.getKey, config.getValue)
        sinkConfig.add(config.getKey, config.getValue)
      }
    })

    /**
     * spark session 配置信息
     */
    val sparkConfig = SparkCommon.sparkConfig
    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${config.getTaskID}")

    val spark = SparkCommon.loadSession(sparkConfig)

    // 记录原始字段映射 sink中write前使用
    val originColumnMap = config.getColumnMap
    // todo:增量字段处理 _1
    val sourceConfigFromParam = config.getSource.getConfig.getAsJsonObject
    lazy val offsetColumnOrigin = sourceConfigFromParam.get("offsetColumns").getAsString
    if (sourceConfigFromParam.get("readMode") != null && "incr".equals(sourceConfigFromParam.get("readMode").getAsString)) {
      // 未选添加进 map
      var newColumnMap = Map[String, String]()
      config.getColumnMap.split(",").foreach(x => {
        newColumnMap += (x.split(":")(0) -> x.split(":")(1))
      })
      if (!newColumnMap.contains(offsetColumnOrigin)) {
        newColumnMap += (offsetColumnOrigin -> offsetColumnOrigin)
      }
      val offsetList: ListBuffer[String] = new ListBuffer[String]
      newColumnMap.foreach(kv => offsetList += kv._1 + ":" + kv._2)
      config.setColumnMap(offsetList.mkString(","))
    }

    val source = PluginUtil.createSource(config.getSource.getType, config.getSource.getConfig)
    val sink = PluginUtil.createSink(config.getSink.getType, config.getSink.getConfig)


    // source 端参数补全
    val sourceConfigMap: util.Map[String, AnyRef] = source.getConfig.asInstanceOf[EtlConfig].getConfigMap

    // todo:增量字段处理 _2
    lazy val offsetColumns = source.getConfig.asInstanceOf[EtlConfig].getConfigMap.get("offsetColumns").asInstanceOf[String]
    if (sourceConfigMap.get("readMode") != null && sourceConfigMap.get("readMode") == "incr" && offsetColumns != null) {
      var newColumnMap = Map[String, String]()
      config.getColumnMap.split(",").foreach(x => {
        newColumnMap += (x.split(":")(0) -> x.split(":")(1))
      })

      sourceConfigMap.put("offsetColumns", newColumnMap(offsetColumns))
    }

    val InSourceConfigEle = new GsonBuilder().create.fromJson(
      new GsonBuilder().disableHtmlEscaping().create().toJson(sourceConfigMap), new TypeToken[JsonElement] {}.getType).asInstanceOf[JsonElement]

    // sink 端参数补全
    val sinkConfigMap = sink.getConfig.asInstanceOf[EtlConfig].getConfigMap

    val InSinkConfigEle = new GsonBuilder().create.fromJson(
      new GsonBuilder().disableHtmlEscaping().create().toJson(sinkConfigMap), new TypeToken[JsonElement] {}.getType).asInstanceOf[JsonElement]

    //val InSourceConfigEle = new GsonBuilder().create.fromJson(source.getConfig.toString,new TypeToken[JsonElement]{}.getType).asInstanceOf[JsonElement]
    //val InSinkConfigEle = new GsonBuilder().create.fromJson(sink.getConfig.toString,new TypeToken[JsonElement]{}.getType).asInstanceOf[JsonElement]

    config.getSource.setConfig(InSourceConfigEle)
    config.getSink.setConfig(InSinkConfigEle)

    val sourceDf: Dataset[Row] = source.asInstanceOf[EtlSource[EtlConfig]].getData(spark, config)

    sink.asInstanceOf[EtlSink[EtlConfig]].output(spark, sourceDf, config, originColumnMap)
  }
}
