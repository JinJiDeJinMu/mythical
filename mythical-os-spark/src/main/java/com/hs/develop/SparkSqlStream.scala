package com.hs.develop

import com.alibaba.fastjson.JSON
import com.hs.common.{SparkCommon, WriteMode}
import com.hs.config.SystemConfig
import com.hs.utils.DeltaMergeUtil.sortConfig
import com.hs.utils.FrameWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable


object SparkSqlStream {

  val checkpointPathFormat = "/delta/events/_checkpoints/delta2delta/%s"

  def main(args: Array[String]): Unit = {
    println("args = " + args(0))
    val params = JSON.parseObject(args(0))

    val executeSql = params.getString("sql").replaceAll("hasmap.", "").replaceAll("hsmap.", "").toLowerCase
    val checkpointPath = checkpointPathFormat.format(params.getString("taskID"))

    var writeMode = ""
    var selectSql = executeSql.substring(executeSql.indexOf("select"), executeSql.lastIndexOf("options"))
    val dt = executeSql.substring(executeSql.indexOf("insert into table"), executeSql.indexOf("select")).substring(17).trim.split(".")
    val optionsMap = getOptions(executeSql)
    val targetTablePath = SystemConfig.tablePathFormat.format(dt(0), dt(1))

    /**
     * spark
     */
    val sparkConfig = SparkCommon.sparkConfig
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)
    spark.sparkContext.setLogLevel("WARN")

    val localPlan = spark.sessionState.sqlParser.parsePlan(selectSql)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    val tableList = localPlan.collect { case r: UnresolvedRelation => r.tableName }

    tableList.foreach(x => {
      val ss = x.split(".")
      val renameDT = ss(0) + "_" + ss(1)
      spark.readStream.format("delta")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .load(SystemConfig.tablePathFormat.format(ss(0), ss(1)))
        .createOrReplaceTempView(renameDT)

      selectSql = selectSql.replaceAll(x, renameDT)
    })

    if (optionsMap.contains("writeMode") && optionsMap.get("writeMode").equals(WriteMode.update.name())) {
      writeMode = WriteMode.update.name()
    }

    val finalDF = spark.sql(selectSql)

    if (writeMode.equalsIgnoreCase(WriteMode.append.name())) {
      finalDF.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointPath)
        .start(targetTablePath)

    } else if (writeMode.equalsIgnoreCase(WriteMode.update.name())) {
      finalDF.writeStream
        .option("checkpointLocation", checkpointPath)
        .foreachBatch { (batchDF: DataFrame, batchId: Long) => {
          val writer = new FrameWriter(batchDF, writeMode, Option(null))
          writer.write(targetTablePath, optionsMap.get("mergeKeys"), sortConfig(writeMode, optionsMap.get("mergeKeys").get))
        }
        }.start()

    } else {
      throw new RuntimeException("不支持的writeMode ," + writeMode)
    }
    spark.streams.awaitAnyTermination()
  }

  def getOptions(sql: String): mutable.Map[String, String] = {
    val map = new mutable.HashMap[String, String]
    if (sql.contains("options")) {
      val options = sql.substring(sql.lastIndexOf("options"))

      val str = options.substring(options.indexOf("("), options.lastIndexOf(")"))
      val strings = str.split("\n").foreach(e => {
        val ss = e.split("=")
        map.put(ss(0).trim, ss(1).trim)
      })
    }
    map
  }

}
