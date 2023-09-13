/*
 *@Author   : DoubleTrey
 *@Time     : 2022/10/27 16:19
 */

package com.hs.utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, desc, row_number}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable.ArrayBuffer

/**
 *
 */
object DeltaMergeUtil {
  /**
   * 指定更新的字段集合, 生成更新表达式 Map
   *
   * @param cols        [[Seq]]
   * @param sourceAlias [[String]]
   * @return [[Map]]
   */
  def updateClause(cols: Seq[String], sourceAlias: String): Map[String, String] = {
    cols.map(x => (s"$x", s"$sourceAlias.$x")).toMap
  }

  /**
   * 增量数据做去重操作，全量数据暂时不做去重
   *
   * @param dataFrame       [[DataFrame]]
   * @param targetTablePath [[String]]  目标 delta 表路径, e.g., /user/hive/warehouse/ods.db/xx_table
   * @param mergeKeys       [[String]]  merge 条件字段, e.g., "id,type"
   * @param sortCols        [[String]]  dataFrame 排序字段, 用于去重过滤 e.g., "update_time:-1,type:1"
   */
  def merge(dataFrame: DataFrame, targetTablePath: String, mergeKeys: String, sortCols: Column*): Unit = {
    var distinctDF: DataFrame = null
    if (sortCols == null) {
      distinctDF = dataFrame
    } else {
      distinctDF = deduplicateDF(dataFrame, mergeKeys, sortCols: _*)
    }
    val clause = updateClause(distinctDF.schema, "s", Seq())

    //distinctDF.show()
    val targetTable: DeltaTable = DeltaTable.forPath(targetTablePath)

    // insertAll 需要字段对齐
    targetTable.as("t")
      .merge(
        distinctDF.as("s"),
        mergeCondition(mergeKeys, "s", "t"))
      .whenMatched().updateExpr(updateClauseExcludeCreateTime(clause))
      //.whenNotMatched().insertAll()
      .whenNotMatched().insertExpr(clause)
      .execute()
  }

  /**
   * 根据 mergeKeys 生成 merge condition
   *
   * @param mergeKeys   [[String]]
   * @param sourceAlias [[String]]
   * @param targetAlias [[String]]
   * @return condition  [[String]]
   */
  def mergeCondition(mergeKeys: String, sourceAlias: String, targetAlias: String): String = {
    val mergeKeyArray = convertMergeKeys(mergeKeys)
    mergeKeyArray.map(x => s"$targetAlias.$x = $sourceAlias.$x").mkString(" and ")
  }

  /**
   * 根据指定的 schema, 过滤掉额外配置的排除字段, 生成更新表达式 Map
   *
   * @param schema [[StructType]]
   * @return [[Map]]
   */
  def updateClause(schema: StructType, sourceAlias: String, excludeCols: Seq[String]): Map[String, String] = {
    schema.map(x => x.name)
      .diff(excludeCols)
      .map(x => (s"$x", s"$sourceAlias.$x"))
      .toMap
  }

  /**
   * 不更新 create_time
   *
   * @param clause [[Map]]
   * @return [[Map]]
   */
  def updateClauseExcludeCreateTime(clause: Map[String, String]): Map[String, String] = {
    clause - "create_time"
  }

  /**
   * 对输入的 dataFrame 数据内容去重
   *
   * @param dataFrame [[DataFrame]]
   * @param mergeKeys [[String]]
   * @param orderExpr 排序表达式，去重结果取同组排序第一条
   * @return [[DataFrame]]
   */
  def deduplicateDF(dataFrame: DataFrame, mergeKeys: String, orderExpr: Column*): DataFrame = {
    val mergeKeyArray = convertMergeKeys(mergeKeys)

    dataFrame.withColumn(
      "rn",
      row_number().over(Window.partitionBy(mergeKeyArray.map(x => col(x)): _*).orderBy(orderExpr: _*))
    ).filter("rn = 1")
      .drop("rn")
  }

  def convertMergeKeys(mergeKeys: String): Array[String] = {
    mergeKeys.split(",").map(x => x.trim())
  }

  /**
   * 根据 mergeKeys 生成 merge condition
   *
   * @param mergeKeys   [[String]]
   * @param sourceAlias [[String]]
   * @param targetAlias [[String]]
   * @return condition  [[String]]
   */
  def matchedCondition(mergeKeys: String, columnWithoutKeys: String, sourceAlias: String, targetAlias: String): String = {
    val columnWithoutKeysArray = columnWithoutKeys.split(",")
    columnWithoutKeysArray.map(x => s"$targetAlias.$x != $sourceAlias.$x").mkString(" or ")
  }

  def sortConfig(mode: String, offsetColumns: String): Option[Seq[Column]] = {
    mode.toLowerCase() match {
      case "append" => None
      case "overwrite" => None
      case "ignore" => None
      case "errorifexists" => None
      case "update" => if (offsetColumns == null) {
        //throw new IllegalArgumentException("更新模式下, 增量字段不能为空")
        None
      } else {
        Some(orderExpr(s"${offsetColumns.split(",")(0)}:-1"))
      }
    }
  }

  /**
   * 根据排序配置, 生成排序逻辑
   *
   * @param input [[String]]  字段排序配置，example: "update_time:-1,type:1"
   * @return [[ArrayBuffer]]
   */
  def orderExpr(input: String): Seq[Column] = {
    val inputArray = input.split(",")
    val resArray = new ArrayBuffer[Column]()
    for (x <- inputArray) {
      val split = x.split(":")
      split(1).trim match {
        case "1" => resArray.append(asc(split(0).trim.toLowerCase()))
        case "-1" => resArray.append(desc(split(0).trim.toLowerCase()))
        case _ => throw new IllegalArgumentException("不支持的字段排序类型[%s]! `-1` for `desc`; `1` for `asc`".format(split(1)))
      }
    }

    resArray
  }
}
