package com.nebula.hs.etl.source

import com.crealytics.spark.excel.ExcelDataFrameReader
import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.ExcelSourceConfig
import com.hs.utils.{Supplement, Transformer}
import org.apache.spark.sql._

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class ExcelSource extends EtlSource[ExcelSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {
    /**
     * spark read Excel options
     */
    val readerOptions = Map(
      "header" -> config.getIncludeHeader,
    )


    var df: DataFrame = spark.read.excel().options(readerOptions).load(config.getFilePath)

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
    var sinkDf = spark.sql(Transformer.fieldNameTransform(config.getColumnMap, config.getTaskID))

    /**
     * 系统字段填充
     */
    sinkDf = Supplement.systemFieldComplete(sinkDf)

    sinkDf
  }
}
