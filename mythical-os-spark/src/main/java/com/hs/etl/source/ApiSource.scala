package com.hs.etl.source

import cn.hutool.http.HttpRequest
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONPath}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.ApiSourceConfig
import com.hs.utils.{Supplement, Transformer}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SparkSession, _}

import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class ApiSource extends EtlSource[ApiSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {

    /**
     * 读取api数据
     */
    val str = readData(config)

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
    tranDF.createOrReplaceTempView("tmpTable_" + config.getTaskID)
    var sql = Transformer.fieldNameTransform(jobConfig.getColumnMap, config.getTaskID)
    if (config.getDataMode.equalsIgnoreCase("oneData")) {
      sql = sql + " limit 1";
    }
    var finalDF = spark.sql(sql)
    //.withColumn("insert_time", current_timestamp())

    /**
     * 系统字段填充
     */
    finalDF = Supplement.systemFieldComplete(finalDF)

    finalDF
  }

  def readData(apiToDeltaJobConfig: ApiSourceConfig): String = {
    val httpRequest = apiToDeltaJobConfig.getMethod match {
      case "get" => HttpRequest.get(apiToDeltaJobConfig.getApiUrl)
      case "post" => HttpRequest.post(apiToDeltaJobConfig.getApiUrl)
      case _ => throw new RuntimeException("restApi只支持get、post方法")
    }
    try {
      if (apiToDeltaJobConfig.getHeader != null && apiToDeltaJobConfig.getHeader.nonEmpty) {
        val header = new String(Base64.getDecoder().decode(apiToDeltaJobConfig.getHeader), StandardCharsets.UTF_8)
        httpRequest.headerMap(toMap(header), true)
      }

      if (apiToDeltaJobConfig.getParameters != null && apiToDeltaJobConfig.getParameters.nonEmpty) {
        val parameters = new String(Base64.getDecoder().decode(apiToDeltaJobConfig.getParameters), StandardCharsets.UTF_8)
        httpRequest.body(parameters)
      }

      val http = apiToDeltaJobConfig.getAuthType match {
        case "Basic Auth" => httpRequest.basicAuth(apiToDeltaJobConfig.getUsername, apiToDeltaJobConfig.getPassword)
        case "Token Auth" => httpRequest.bearerAuth(apiToDeltaJobConfig.getToken)
        case _ => httpRequest
      }
      val res = http.execute()

      if (!res.isOk) {
        throw new RuntimeException("api响应失败,message ->" + res.body())
      }
      var response = res.body()

      println("api result = " + response)

      if (apiToDeltaJobConfig.getDataPath != null && apiToDeltaJobConfig.getDataPath.nonEmpty) {
        response = JSON.toJSONString(JSONPath.eval(response, apiToDeltaJobConfig.getDataPath), SerializerFeature.WriteMapNullValue)
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
