package com.hs.etl.sink

import com.google.gson.JsonObject
import com.hs.etl.EtlPlugin
import com.hs.etl.config.EtlConfig
import com.hs.etl.config.sink.DeltaSinkConfig
import com.hs.utils.Supplement
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
trait EtlSink[T <: EtlConfig] extends EtlPlugin[T] {
  // fixme:路径考虑兼容老数据
  val offsetPath = "/delta/_offset/etl/%s"

  def output(spark: SparkSession, data: Dataset[Row], jobConfig: EtlConfig, otherInFor: String): Unit

  /**
   * 水印添加
   *
   * @param ds        [[Dataset]].[[Row]]
   * @param jobConfig [[EtlConfig]] and [[EtlConfig]].SinkConfig.watermark
   * @return new df
   */
  def watermarkI(ds: mutable.Map[String, DataFrame], jobConfig: EtlConfig): mutable.Map[String, DataFrame] = {
    val df: mutable.Map[String, DataFrame] = ds
    if (jobConfig.getSink.getWatermarkConfigs != null && !jobConfig.getSink.getWatermarkConfigs.isEmpty) {
      jobConfig.getSink.getWatermarkConfigs.asScala.toList.foreach(ele => {
        /* ele.getTableName  fixme: 待兼容多表的时候使用 */
        if (ele.getIsWatermark.equals("1") && ele.getTableName != "") {
          if (df.contains(ele.getTableName)) {
            df(ele.getTableName) = Supplement.traceData(jobConfig.getTaskID, df(ele.getTableName))
          }
        }
        df
      })
    }
    df
  }

  /**
   * 数据量及存储量统计
   *
   * @param ds        [[Dataset]].[[Row]]
   * @param jobConfig [[EtlConfig]] and [[EtlConfig]].SinkConfig
   */
  def dataCountI(ds: Dataset[Row], jobConfig: EtlConfig): Unit = {
    if (jobConfig.getExtraConfig != null
      && StringUtils.isNotBlank(jobConfig.getExtraConfig.getDataCountKafkaServers)
      && StringUtils.isNotBlank(jobConfig.getExtraConfig.getDataCountTopicName)) {
      // 仅数据写入delta 计算存储量 其它情况下功能待扩展
      if (jobConfig.getSink.getType == "delta") {
        val deltaSinkConfig = config.asInstanceOf[DeltaSinkConfig]
        Supplement.dataCount(jobConfig.getTaskID, jobConfig.getSource.getType + "->" + jobConfig.getSink.getType
          , ds.count(), jobConfig.getExtraConfig.getDataCountKafkaServers, jobConfig.getExtraConfig.getDataCountTopicName, deltaSinkConfig.getTargetDatabase)
      } else {
        Supplement.dataCount(jobConfig.getTaskID, jobConfig.getSource.getType + "->" + jobConfig.getSink.getType
          , ds.count(), jobConfig.getExtraConfig.getDataCountKafkaServers, jobConfig.getExtraConfig.getDataCountTopicName)
      }
    }
  }

  /**
   * 任务成功后,保存offset值
   *
   * @param ds        [[Dataset]].[[Row]]
   * @param jobConfig [[EtlConfig]].SourceConfig
   */
  def saveOffsetI(ds: Dataset[Row]
                  , jobConfig: EtlConfig): Unit = {
    var maxOffsetDf: DataFrame = null
    val sourceConfigJson: JsonObject = jobConfig.getSource.getConfig.getAsJsonObject

    if (sourceConfigJson.get("readMode") == null) return

    // 获取待存储的的offset值
    if ("incr".equals(sourceConfigJson.get("readMode").getAsString)) {
      val offsetColumns = sourceConfigJson.get("offsetColumns").getAsString
      maxOffsetDf = ds.selectExpr(s"max(${offsetColumns}) as offset ").withColumn("taskID", lit(jobConfig.getTaskID))
      println("任务执行成功,存储增量值: " + maxOffsetDf.collectAsList())
    }
    //更新offset 空df不写入null
    if ("incr".equals(sourceConfigJson.get("readMode").getAsString) && !maxOffsetDf.isEmpty) {
      maxOffsetDf.write.option("header", value = true).option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS").mode(SaveMode.Overwrite).csv(offsetPath.format(jobConfig.getTaskID))
    }
  }
}
