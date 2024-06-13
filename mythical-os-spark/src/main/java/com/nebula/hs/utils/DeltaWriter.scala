/*
 *@Author   : DoubleTrey
 *@Time     : 2022/11/7 15:54
 */

package com.nebula.hs.utils

import com.hs.utils.DeltaMergeUtil.merge
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql._

import java.util.concurrent.TimeUnit

/**
 *
 * @param dataFrame     [[DataFrame]]
 * @param writeMode     [[String]]
 * @param writerOptions [[Map]]
 * @param interval      Option[[Long]]
 * @param timeUnit      Option[[TimeUnit]]
 */
final class StreamWriter(dataFrame: DataFrame, writeMode: String, writerOptions: Option[Map[String, String]], interval: Option[Long], timeUnit: Option[TimeUnit]) {
  private var writer: DataStreamWriter[Row] = dataFrame.writeStream.format("delta")
  if (writerOptions.isDefined) {
    writer = writer.options(writerOptions.get)
  }
  if (interval.isDefined) {
    writer = writer.trigger(Trigger.ProcessingTime(interval.get, timeUnit.getOrElse(default = TimeUnit.SECONDS)))
  }

  def write(targetTablePath: String, mergeKeys: Option[String], sortCols: Option[Seq[Column]]): Unit = {
    /**
     * Fixme: 空 DataFrame 过滤，避免空 parquet 文件
     */
    writeMode.toLowerCase() match {
      case "append" => append(targetTablePath)
      case "update" => update(targetTablePath, mergeKeys.get, sortCols.get: _*)
      case _ => throw new IllegalArgumentException("不支持的数据写入方式[%s]".format(writeMode))
    }
  }

  def append(targetTablePath: String): Unit = {
    writer.outputMode(OutputMode.Append).start(targetTablePath)
  }

  def update(targetTablePath: String, mergeKeys: String, sortCols: Column*): Unit = {
    def batchWrite(batchDF: DataFrame, batchId: Long): Unit = {
      merge(batchDF, targetTablePath, mergeKeys, sortCols: _*)
    }

    writer.foreachBatch(batchWrite _).start()
  }
}

/**
 * StreamWriter 类伴生对象
 */
object StreamWriter {
  def apply(dataFrame: DataFrame, writeMode: String, writerOptions: Option[Map[String, String]], interval: Option[Long], timeUnit: Option[TimeUnit]): StreamWriter = new StreamWriter(dataFrame, writeMode, writerOptions, interval, timeUnit)
}

/**
 *
 * @param dataFrame     [[DataFrame]]
 * @param writeMode     [[String]]
 * @param writerOptions Option[[Map]]
 */
final class FrameWriter(dataFrame: DataFrame, writeMode: String, writerOptions: Option[Map[String, String]]) {
  private lazy val writer: DataFrameWriter[Row] = {
    var _writer = dataFrame.write.format("delta")

    if (writerOptions.isDefined) {
      _writer = _writer.options(writerOptions.get)
    }
    _writer
  }

  def write(targetTablePath: String, mergeKeys: Option[String], sortCols: Option[Seq[Column]]): Unit = {
    /**
     * Fixme: 空 DataFrame 过滤，避免空 parquet 文件
     */
    writeMode.toLowerCase() match {
      case "append" => save(SaveMode.Append, targetTablePath)
      case "overwrite" => save(SaveMode.Overwrite, targetTablePath)
      case "ignore" => save(SaveMode.Ignore, targetTablePath)
      case "errorifexists" => save(SaveMode.ErrorIfExists, targetTablePath)
      case "update" => update(targetTablePath, mergeKeys.get, sortCols.orNull: _*)
      case _ => throw new IllegalArgumentException("不支持的数据写入方式[%s]".format(writeMode))
    }
  }

  private def save(saveMode: SaveMode, targetTablePath: String): Unit = {
    writer.mode(saveMode).save(targetTablePath)
  }

  def update(targetTablePath: String, mergeKeys: String, sortCols: Column*): Unit = {
    merge(dataFrame, targetTablePath, mergeKeys, sortCols: _*)
  }
}

/**
 * FrameWriter 类伴生对象
 */
object FrameWriter {
  def apply(dataFrame: DataFrame, writeMode: String, writerOptions: Option[Map[String, String]]): FrameWriter = new FrameWriter(dataFrame, writeMode, writerOptions)
}