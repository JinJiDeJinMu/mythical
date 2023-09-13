/*
 *@Author   : DoubleTrey
 *@Time     : 2022/11/24 11:49
 */

package com.hs.utils

import org.apache.spark.sql._

//class JdbcWriter(dataFrame: DataFrame, writeMode: String, writerOptions: Map[String, String]) {
class JdbcWriter(dataFrame: DataFrame, writeMode: String, writerOptions: Map[String, String], enableBreakpointResume: Boolean = false, breakpointResumeColumn: String = "insert_time", spark: SparkSession = null, taskID: String = System.currentTimeMillis().toString) {
  private lazy val writer: DataFrameWriter[Row] = {
    var _writer = dataFrame.write.format("jdbc")

    if (writerOptions.nonEmpty) {
      _writer = _writer.options(writerOptions)
    }

    /**
     * 防止overwrite模式改变目标表的结构
     */
    if (writeMode.equalsIgnoreCase("overwrite")) {
      _writer.option("truncate", "true")
    }
    _writer
  }


  def write(mergeKeys: String): Unit = {
    writeMode.toLowerCase() match {
      case "append" => save(SaveMode.Append)
      case "overwrite" => save(SaveMode.Overwrite)
      case "ignore" => save(SaveMode.Ignore)
      case "errorifexists" => save(SaveMode.ErrorIfExists)
      case "update" => update(mergeKeys)
      case _ => throw new IllegalArgumentException("不支持的数据写入方式[%s]".format(writeMode))
    }
  }

  private def save(saveMode: SaveMode): Unit = {
    writer.mode(saveMode).save()
  }

  def update(mergeKeys: String): Unit = {
    FsJdbcUtils.upsertTable(dataFrame, writerOptions, mergeKeys, enableBreakpointResume = enableBreakpointResume, breakpointResumeColumn = breakpointResumeColumn, spark = spark, taskId = taskID)
  }
}

