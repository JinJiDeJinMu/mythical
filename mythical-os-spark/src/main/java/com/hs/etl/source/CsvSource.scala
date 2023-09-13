package com.hs.etl.source

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.CsvSourceConfig
import com.hs.utils.{Supplement, Transformer}
import org.apache.spark.sql._

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class CsvSource extends EtlSource[CsvSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {
    /**
     * spark read csv options
     */
    val readerOptions = Map(
      "delimiter" -> config.getDelimiter,
      "header" -> config.getIncludeHeader,
      //FIXME: 需要传字符集参数
      "encoding" -> config.getEncoding,
      "header" -> config.getIncludeHeader,
      "multiLine" -> "true",
      "escape" -> "\""
    )

    var df = spark.read.options(readerOptions).csv(config.getFilePath)

    /**
     * 控制列数限制
     */
    val csvColumnLen = df.schema.count(x => true)
    val inColumnLen = config.getColumnMap.split(",").length
    if (csvColumnLen < inColumnLen) {
      throw new IllegalArgumentException(s"传入列数为：$inColumnLen 大于csv文件的列数：$csvColumnLen,需保证小于或等于")
    }

    /**
     * spark自生成表头处理
     */
    if ("false".equals(config.getIncludeHeader)) {
      for (x <- df.schema) {
        df = df.withColumnRenamed(x.name, x.name.replace("_c", "column"))
      }
    }

    df.createOrReplaceTempView("tmpTable_" + config.getTaskID)
    var sinkDf = spark.sql(Transformer.fieldNameTransform(jobConfig.getColumnMap, config.getTaskID))
    //sinkDf = sinkDf.withColumn("insert_time", current_timestamp())

    /**
     * 系统字段填充
     */
    sinkDf = Supplement.systemFieldComplete(sinkDf)
    sinkDf
  }
}
