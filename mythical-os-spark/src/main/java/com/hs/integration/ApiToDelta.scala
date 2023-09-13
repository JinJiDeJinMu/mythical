package com.hs.integration

import cn.hutool.http.HttpRequest
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONPath}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.hs.common.SparkCommon
import com.hs.config.ApiToDeltaJobConfig
import com.hs.utils.DeltaMergeUtil.orderExpr
import com.hs.utils.{FrameWriter, Supplement, Transformer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64


object ApiToDelta {
  val jobType = "Api2Delta"

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[ApiToDeltaJobConfig]

    /**
     * spark session 配置信息
     */
    val sparkConfig = SparkCommon.sparkConfig
    sparkConfig.setIfMissing("spark.app.name", s"$jobType-${jobConf.taskID}")

    /**
     * Delta SparkSession
     */
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)

    /**
     * 读取api数据
     */
    val str = readData(jobConf)

    if (str == null || str.isEmpty) {
      throw new RuntimeException("api响应体为空或者解析响应体路径配置不正确")
    }
    import spark.implicits._

    val df = spark.read.json(spark.sparkContext.makeRDD(str :: Nil).toDS())

    df.printSchema()

    var tranDF = df
    for (colName <- df.columns) {
      tranDF = tranDF.withColumn(colName, col(colName).cast(StringType))
    }
    tranDF.printSchema()

    /**
     * 字段映射转换
     */
    tranDF.createOrReplaceTempView("tmpTable_" + jobConf.taskID)
    var sql = Transformer.fieldNameTransform(jobConf.columnMap, jobConf.taskID)
    if (jobConf.dataMode.equalsIgnoreCase("oneData")) {
      sql = sql + " limit 1";
    }
    var finalDF = spark.sql(sql)
    //.withColumn("insert_time", current_timestamp())

    /**
     * 系统字段填充
     */
    finalDF = Supplement.systemFieldComplete(finalDF)

    /**
     * 分区字段处理
     */
    finalDF = Supplement.partitionTrans(finalDF, jobConf.zoneType, jobConf.zoneFieldCode, jobConf.zoneTargetFieldCode, jobConf.zoneTypeUnit)

    /**
     * 敏感字段加密处理
     */
    finalDF = Supplement.encryptTrans(finalDF, jobConf.encrypt)

    val targetTablePath = jobConf.tablePathFormat.format(jobConf.targetDatabase, jobConf.targetTable)
    val writer = new FrameWriter(finalDF, jobConf.writeMode, Option(null))
    writer.write(targetTablePath, Option(jobConf.mergeKeys), Option(orderExpr(jobConf.sortColumns)))

    /**
     * 接入数据量统计
     */
    Supplement.dataCount(jobConf.taskID, "api", finalDF.count(), jobConf.dataCountKafkaServers, jobConf.dataCountTopicName, jobConf.targetDatabase)
  }

  def readData(apiToDeltaJobConfig: ApiToDeltaJobConfig): String = {
    val httpRequest = apiToDeltaJobConfig.method match {
      case "get" => HttpRequest.get(apiToDeltaJobConfig.apiUrl)
      case "post" => HttpRequest.post(apiToDeltaJobConfig.apiUrl)
      case _ => throw new RuntimeException("restApi只支持get、post方法")
    }
    try {
      if (apiToDeltaJobConfig.header != null && apiToDeltaJobConfig.header.nonEmpty) {
        val header = new String(Base64.getDecoder().decode(apiToDeltaJobConfig.header), StandardCharsets.UTF_8)
        httpRequest.headerMap(toMap(header), true)
      }

      if (apiToDeltaJobConfig.parameters != null && apiToDeltaJobConfig.parameters.nonEmpty) {
        val parameters = new String(Base64.getDecoder().decode(apiToDeltaJobConfig.parameters), StandardCharsets.UTF_8)
        httpRequest.body(parameters)
      }

      val http = apiToDeltaJobConfig.authType match {
        case "Basic Auth" => httpRequest.basicAuth(apiToDeltaJobConfig.username, apiToDeltaJobConfig.password)
        case "Token Auth" => httpRequest.bearerAuth(apiToDeltaJobConfig.token)
        case _ => httpRequest
      }
      val res = http.execute()

      if (!res.isOk) {
        throw new RuntimeException("api响应失败,message ->" + res.body())
      }
      var response = res.body()

      println("api result = " + response)

      if (apiToDeltaJobConfig.dataPath != null && apiToDeltaJobConfig.dataPath.nonEmpty) {
        response = JSON.toJSONString(JSONPath.eval(response, apiToDeltaJobConfig.dataPath), SerializerFeature.WriteMapNullValue)
      }
      response
    } catch {
      case ex: Exception => throw new RuntimeException("读取api数据异常,message ->" + ex.getMessage)
    }
  }

  def toMap(str: String): util.Map[String, String] = try {
    val gson: Gson = new Gson
    gson.fromJson(str, new TypeToken[util.Map[String, String]]() {}.getType)
  } catch {
    case e: Exception =>
      throw new RuntimeException("参数解析异常,message ->" + e.getMessage)
  }

}
