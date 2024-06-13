package com.nebula.hs.develop

import com.alibaba.fastjson.JSON
import com.hs.common.SparkCommon
import com.hs.config.EncryptColConfig
import com.hs.utils.Supplement
import org.apache.spark.sql.SparkSession

/**
 * spark sql execute
 */
object InternalSparkSqlV2 {

  def main(args: Array[String]): Unit = {
    println("args = " + args(0))
    val params = JSON.parseObject(args(0))

    val sparkConfig = SparkCommon.sparkConfig
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)

    val executeSql = params.getString("sql").replaceAll("hasmap.", "").replaceAll("hsmap.", "")
    println("execute sql = " + executeSql)
    var resultDF = spark.sql(executeSql)

    /**
     * 敏感字段加密处理
     */
    import scala.collection.JavaConverters._
    resultDF = Supplement.encryptTrans(resultDF, params.getJSONArray("encrypt").toJavaList(classOf[EncryptColConfig]).asScala.toList)
    resultDF.show()
  }
}
