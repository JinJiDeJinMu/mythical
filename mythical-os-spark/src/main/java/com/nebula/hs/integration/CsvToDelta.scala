/*
 *@Author   : DoubleTrey
 *@Time     : 2022/11/17 10:18
 */

package com.nebula.hs.integration

import com.hs.common.SparkCommon
import com.hs.config.CsvToDeltaJobConfig
import com.hs.utils.DeltaMergeUtil.orderExpr
import com.hs.utils.{FrameWriter, Supplement, Transformer}
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods


object CsvToDelta {
  val jobType = "Csv2Delta"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[CsvToDeltaJobConfig]

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
     * spark read csv options
     */
    val readerOptions = Map(
      "delimiter" -> jobConf.delimiter,
      "header" -> jobConf.includeHeader,
      //FIXME: 需要传字符集参数
      "encoding" -> jobConf.encoding,
      "header" -> jobConf.includeHeader,
      "multiLine" -> "true",
      "escape" -> "\""
    )

    var df = spark.read.options(readerOptions).csv(jobConf.filePath)

    /**
     * 控制列数限制
     */
    val csvColumnLen = df.schema.count(x => true)
    val inColumnLen = jobConf.columnMap.split(",").length
    if (csvColumnLen < inColumnLen) {
      throw new IllegalArgumentException(s"传入列数为：$inColumnLen 大于csv文件的列数：$csvColumnLen,需保证小于或等于")
    }

    /**
     * spark自生成表头处理
     */
    if ("false".equals(jobConf.includeHeader)) {
      for (x <- df.schema) {
        df = df.withColumnRenamed(x.name, x.name.replace("_c", "column"))
      }
    }

    df.createOrReplaceTempView("tmpTable_" + jobConf.taskID)
    var sinkDf = spark.sql(Transformer.fieldNameTransform(jobConf.columnMap, jobConf.taskID))
    //sinkDf = sinkDf.withColumn("insert_time", current_timestamp())

    /**
     * 系统字段填充
     */
    sinkDf = Supplement.systemFieldComplete(sinkDf)

    /**
     * 分区字段处理
     */
    sinkDf = Supplement.partitionTrans(sinkDf, jobConf.zoneType, jobConf.zoneFieldCode, jobConf.zoneTargetFieldCode, jobConf.zoneTypeUnit)

    /**
     * 敏感字段加密处理
     */
    sinkDf = Supplement.encryptTrans(sinkDf, jobConf.encrypt)

    // 入湖
    val targetTablePath = jobConf.tablePathFormat.format(jobConf.targetDatabase, jobConf.targetTable)
    val writer = new FrameWriter(sinkDf, jobConf.writeMode.toLowerCase, Option(null))
    // 2022-11-25: 应用端传递了 sortColumns
    writer.write(targetTablePath, Option(jobConf.mergeKeys), Option(orderExpr(jobConf.sortColumns)))

    /**
     * 接入数据量统计
     */
    Supplement.dataCount(jobConf.taskID, "csv", sinkDf.count(), jobConf.dataCountKafkaServers, jobConf.dataCountTopicName, jobConf.targetDatabase)
  }
}
