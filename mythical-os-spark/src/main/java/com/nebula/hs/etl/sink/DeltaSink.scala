package com.nebula.hs.etl.sink

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import com.hs.etl.PartitionsConfig
import com.hs.etl.config.EtlConfig
import com.hs.etl.config.sink.DeltaSinkConfig
import com.hs.utils.DeltaMergeUtil.sortConfig
import com.hs.utils.{FrameWriter, Supplement, Transformer}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.mutable


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class DeltaSink extends EtlSink[DeltaSinkConfig] {

  def output(spark: SparkSession, ds: Dataset[Row], jobConfig: EtlConfig, otherInFor: String): Unit = {
    // drop 非选字段
    val selectCol = mutable.ArrayBuffer[String]()
    otherInFor.split(",").foreach(x => {
      selectCol += (x.split(":")(1))
    })
    //入湖系统内置字段
    var sinkDF = Supplement.systemFieldComplete(ds.selectExpr(selectCol: _*))

    //分区字段处理
    sinkDF = Supplement.partitionMulTrans(sinkDF, jobConfig.getSink.getConfig.getAsJsonObject.get("partitions").getAsJsonArray.toList.map(ele =>
      new GsonBuilder().create.fromJson(ele.toString, new TypeToken[PartitionsConfig] {}.getType).asInstanceOf[PartitionsConfig]))

    //加密
    val targetTable = jobConfig.getSink.getConfig.getAsJsonObject.get("targetTable").getAsString
    sinkDF = Supplement.encryptDecryptTransMul(
      mutable.Map(targetTable -> sinkDF)
      , "encrypt", jobConfig.getSource.getAlgConfig.asScala.toList)(targetTable)

    //内置水印
    sinkDF = watermarkI(mutable.Map(targetTable -> sinkDF), jobConfig)(targetTable)

    //sink to target
    val targetTablePath = config.getTablePathFormat.format(config.getTargetDatabase.toLowerCase(), config.getTargetTable)

    //修复es bug，只有source为es才需要特殊处理
    if (jobConfig.getSource.getType.toLowerCase().equals("es")) {
      val targetTableSchema: StructType = DeltaLog.forTable(spark, targetTablePath).snapshot.schema
      sinkDF = Transformer.fieldNameAndTypeTransformByDeltaSink(sinkDF, jobConfig.getSource.getConfig.getAsJsonObject.get("sourceColumns").toString.replaceAll("\"", ""), config.getColumnMap, targetTableSchema)
    }

    val writer = new FrameWriter(sinkDF, config.getWriteMode.toLowerCase, Option(null))
    writer.write(targetTablePath, Option(config.getMergeKeys), sortConfig(config.getWriteMode, config.getSortColumns))

    /* offset 存储 */
    saveOffsetI(ds, jobConfig)
    /* 入湖数据量统计 */
    dataCountI(sinkDF, jobConfig)
  }
}