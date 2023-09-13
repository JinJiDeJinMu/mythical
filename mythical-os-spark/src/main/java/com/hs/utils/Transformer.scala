/*
 *@Author   : DoubleTrey
 *@Time     : 2022/11/10 11:58
 */

package com.hs.utils

import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * 数据类型转换,字段别名转换
 */
object Transformer {
  /**
   * to_json 转换的字段类型
   */
  private val complexTypeNames = Array("array", "struct", "map")

  /**
   *
   * @param dataFrame [[DataFrame]]
   * @return [[DataFrame]]
   */
  def transform(dataFrame: DataFrame): DataFrame = {
    // 判断是否需要处理，若无需处理，直接返回
    if (dataFrame.schema.count(x => complexTypeNames.contains(x.dataType.typeName)) > 0
      || dataFrame.schema.count(x => x.dataType.typeName.contains("decimal")) > 0) {
      val transColumns = dataFrame.schema.map(x =>
        if (complexTypeNames.contains(x.dataType.typeName)) {
          s"to_json(${x.name}) as ${x.name}"
        } else if (x.dataType.typeName.contains("decimal")) {
          s"cast(${x.name} as decimal(38,8)) as ${x.name}"
        } else {
          x.name
        }
      )
      dataFrame.selectExpr(transColumns: _*)
    } else {
      dataFrame
    }
  }

  /**
   * 字段名称，source和target映射,并把主键提至第一位
   *
   * @param columnMap [[String]]
   * @return [[String]]
   */
  def fieldNameTransform(columnMap: String, taskID: String, mergeKey: String): String = {
    val select = new StringBuilder("select ")

    var mergeKeyStr: String = ""
    columnMap.split(",").foreach(col => {
      val sourceCol = col.split(":")(0)
      val targetCol = col.split(":")(1)
      if (targetCol.equals(mergeKey)) {
        mergeKeyStr = mergeKeyStr + "`" + sourceCol + "`" + " as " + targetCol + ","
      }
    })

    var cols: String = ""
    columnMap.split(",").foreach(col => {
      val sourceCol = col.split(":")(0)
      val targetCol = col.split(":")(1)
      if (!targetCol.equals(mergeKey)) {
        cols = cols + "`" + sourceCol + "`" + " as " + targetCol + ","
      }
    })
    select.append(mergeKeyStr).append(cols.substring(0, cols.length - 1)).append(" from tmpTable_").append(taskID).toString()
  }

  /**
   * 字段名称，source和target映射
   *
   * @param columnMap [[String]]
   * @return [[String]]
   */
  def fieldNameTransform(columnMap: String, taskID: String): String = {
    val stringBuilder = new StringBuilder("select ")
    val str = columnMap.split(",").map(col => {
      "`" + col.split(":")(0) + "`" + " as " + col.split(":")(1)
    }).mkString(",")
    stringBuilder.append(str).append(" from tmpTable_").append(taskID).toString()
  }

  def fieldNameTransformRedis(columnMap: String, taskID: String): String = {
    val stringBuilder = new StringBuilder("select ")
    val str = columnMap.split(",").map(col => {
      val column = if (col.contains(".")) col.split("\\.")(1) else col
      "`" + column.split(":")(0) + "`" + " as " + column.split(":")(1)
    }).mkString(",")
    stringBuilder.append(str).append(" from tmpTable_").append(taskID).toString()
  }

  /**
   * 字段名称，source和target映射
   * select `id` as id,`error_log_count` as error_log_count, `insert_time` as insert_time from tmpTable
   *
   * @param columnMap [[String]]
   * @return [[String]]
   */
  def fieldNameTransformKafka(columnMap: String, taskID: String): String = {
    val stringBuilder = new StringBuilder("select ")
    val str = columnMap.split(",").map(col => {
      "`" + col.split(":")(0) + "`" + " as " + col.split(":")(1)
    }).mkString(",")
    stringBuilder.append(str).append(", `insert_time` as insert_time").append(" from tmpTable_").append(taskID).toString()
  }

  /**
   * 字段名称，source和target映射,字段类型变更
   *
   * @param columnMap [[String]]
   * @return [[String]]
   */
  def fieldNameAndTypeTransformEsToDelta(dataFrame: DataFrame, sourceColumns: String, columnMap: String, esMapping: JsonObject, targetTableSchema: StructType): DataFrame = {
    var columnTypeMap = Map[String, String]()
    //    sourceColumns.split(",").foreach(x => columnTypeMap += (x.split(":")(0) -> x.split(":")(1)))
    sourceColumns.split(",").foreach(x => {
      var value: String = x.split(":")(1)
      value = if (value.toLowerCase().contains("text")) "STRING" else value
      columnTypeMap += (x.split(":")(0) -> value)
    })

    var columnMapFinal = Map[String, String]()
    columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))

    val schema = dataFrame.schema.filter(x => columnTypeMap.contains(x.name))
    val targetTableSchemaMap: Map[String, String] = targetTableSchema.map(field => (field.name, field.dataType.typeName)).toMap

    val transColumns = schema.map(x =>
      if (complexTypeNames.contains(x.dataType.typeName)) {
        s"to_json(${x.name}) as ${columnMapFinal(x.name)}"
      } else if (x.dataType.typeName.toLowerCase().contains("decimal")) {
        s"cast(${x.name} as decimal(38,8)) as ${columnMapFinal(x.name)}"
      } else if (targetTableSchemaMap(columnMapFinal(x.name)).toLowerCase().contains("date")) {
        var esFormat = "yyyy-MM-dd"
        try {
          esFormat = esMapping.getAsJsonObject(x.name).get("format").toString.replaceAll("\"", "")
        } catch {
          case e: Exception => println("获取es data format异常，使用默认format:yyyy-MM-dd")
        }
        s"to_date(${x.name}, '$esFormat') as ${columnMapFinal(x.name)}"
      } else if (targetTableSchemaMap(columnMapFinal(x.name)).toLowerCase().contains("timestamp")) {
        s"to_timestamp(${x.name}) as ${columnMapFinal(x.name)}"
      } else {
        s"cast(${x.name} as ${columnTypeMap(x.name)}) as ${columnMapFinal(x.name)}"
      }
    )
    dataFrame.selectExpr(transColumns: _*)
  }


  def fieldNameAndTypeTransformByDeltaSink(dataFrame: DataFrame, sourceColumns: String, columnMap: String, targetTableSchema: StructType): DataFrame = {
    println("source schema")
    dataFrame.printSchema()

    println("delta schema")
    println(targetTableSchema.mkString)
    var columnTypeMap = Map[String, String]()
    sourceColumns.split(",").foreach(x => {
      var value: String = x.split(":")(1)
      value = if (value.toLowerCase().contains("text")) "STRING" else value
      columnTypeMap += (x.split(":")(0) -> value)
    })

    var columnMapFinal = Map[String, String]()
    columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))

    val schema = dataFrame.schema.filter(x => columnTypeMap.contains(x.name))
    val targetTableSchemaMap: Map[String, String] = targetTableSchema.map(field => (field.name, field.dataType.typeName)).toMap

    val transColumns = schema.map(x =>
      if (complexTypeNames.contains(x.dataType.typeName)) {
        s"to_json(${x.name}) as ${columnMapFinal(x.name)}"
      } else if (x.dataType.typeName.toLowerCase().contains("decimal")) {
        s"cast(${x.name} as decimal(38,8)) as ${columnMapFinal(x.name)}"
      } else if (targetTableSchemaMap(columnMapFinal(x.name)).toLowerCase().contains("date") && x.dataType.typeName.toLowerCase().contains("string")) {
        var esFormat = "yyyy-MM-dd HH:mm:ss"
        s"to_date(${x.name}, '$esFormat') as ${columnMapFinal(x.name)}"
      } else if (targetTableSchemaMap(columnMapFinal(x.name)).toLowerCase().contains("timestamp")) {
        s"to_timestamp(${x.name}) as ${columnMapFinal(x.name)}"
      } else {
        s"cast(${x.name} as ${columnTypeMap(x.name)}) as ${columnMapFinal(x.name)}"
      }
    )
    dataFrame.selectExpr(transColumns: _*)
  }


  //  def fieldNameAndTypeTransformByDeltaSink(dataFrame: DataFrame, sourceColumns: String, columnMap: String, targetTableSchema: StructType): DataFrame = {
  //    println("source schema")
  //    dataFrame.printSchema()
  //
  //    println("delta schema")
  //    println(targetTableSchema)
  //
  //    val targetTableSchemaMap: Map[String, String] = targetTableSchema.map(field => (field.name, field.dataType.typeName)).toMap
  //
  //    val transColumns = dataFrame.schema.map(x =>
  //      if (complexTypeNames.contains(x.dataType.typeName)) {
  //        s"to_json(${x.name}) as ${x.name}"
  //      } else if (x.dataType.typeName.toLowerCase().contains("decimal")) {
  //        s"cast(${x.name} as decimal(38,8)) as ${x.name}"
  //      } else if (targetTableSchemaMap(x.name).toLowerCase().contains("date") && x.dataType.typeName.toLowerCase().contains("string")) {
  //        var esFormat = "yyyy-MM-dd HH:mm:ss"
  //        s"to_date(${x.name}, '$esFormat') as ${x.name}"
  //      } else if (targetTableSchemaMap(x.name).toLowerCase().contains("timestamp")) {
  //        s"to_timestamp(${x.name}) as ${x.name}"
  //      } else {
  //        s"cast(${x.name} as ${x.dataType.typeName}) as ${x.name}"
  //      }
  //    )
  //    dataFrame.selectExpr(transColumns: _*)
  //  }

  /**
   * 字段名称，source和target映射,字段类型变更
   *
   * @param columnMap [[String]]
   * @return [[String]]
   */
  def fieldNameAndTypeTransform(dataFrame: DataFrame, sourceColumns: String, columnMap: String, esMapping: JsonObject): DataFrame = {
    var columnTypeMap = Map[String, String]()
    //    sourceColumns.split(",").foreach(x => columnTypeMap += (x.split(":")(0) -> x.split(":")(1)))
    sourceColumns.split(",").foreach(x => {
      var value: String = x.split(":")(1)
      value = if (value.toLowerCase().contains("text")) "STRING" else value
      columnTypeMap += (x.split(":")(0) -> value)
    })

    var columnMapFinal = Map[String, String]()
    columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))

    val schema = dataFrame.schema.filter(x => columnTypeMap.contains(x.name))

    val transColumns = schema.map(x =>
      if (complexTypeNames.contains(x.dataType.typeName)) {
        s"to_json(${x.name}) as ${columnMapFinal(x.name)}"
      } else if (x.dataType.typeName.toLowerCase().contains("decimal")) {
        s"cast(${x.name} as decimal(38,8)) as ${columnMapFinal(x.name)}"
      } else if (columnTypeMap(x.name).toLowerCase().contains("date")) {
        var esFormat = "yyyy-MM-dd"
        try {
          esFormat = esMapping.getAsJsonObject(x.name).get("format").toString.replaceAll("\"", "").replaceAll("\'T\'", " ")
        } catch {
          case e: Exception => println("获取es data format异常，使用默认format:yyyy-MM-dd")
        }
        s"to_date(${x.name}, '$esFormat') as ${columnMapFinal(x.name)}"
      } else if (columnTypeMap(x.name).toLowerCase().contains("timestamp")) {
        s"to_timestamp(${x.name}) as ${columnMapFinal(x.name)}"
      } else {
        s"cast(${x.name} as ${columnTypeMap(x.name)}) as ${columnMapFinal(x.name)}"
      }
    )
    dataFrame.selectExpr(transColumns: _*)
  }

  def fieldNameAndTypeTransformByMongo(dataFrame: DataFrame, sourceColumns: String, columnMap: String, ignoreCheck: Boolean = false): DataFrame = {
    var columnTypeMap = Map[String, String]()
    sourceColumns.split(",").foreach(x => columnTypeMap += (x.split(":")(0) -> x.split(":")(1)))

    var columnMapFinal = Map[String, String]()
    columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))

    // 判断是否需要处理，若无需处理，直接返回
    if (dataFrame.schema.count(x => complexTypeNames.contains(x.dataType.typeName)) > 0
      || dataFrame.schema.count(x => x.dataType.typeName.contains("decimal")) > 0
      || ignoreCheck) {
      val transColumns = dataFrame.schema.map(x =>
        if (complexTypeNames.contains(x.dataType.typeName)) {
          s"to_json(${x.name}) as ${columnMapFinal(x.name)}"
        } else if (x.dataType.typeName.toLowerCase().contains("decimal")) {
          s"cast(${x.name} as decimal(38,8)) as ${columnMapFinal(x.name)}"
        } else if (columnTypeMap(x.name).toLowerCase().contains("date") || columnTypeMap(x.name).toLowerCase().contains("timestamp")) {
          s"to_timestamp(${x.name}) as ${columnMapFinal(x.name)}"
        } else {
          s"cast(${x.name} as ${columnTypeMap(x.name)}) as ${columnMapFinal(x.name)}"
        }
      )
      dataFrame.selectExpr(transColumns: _*)
    } else {
      dataFrame
    }
  }


  //  def fieldNameAndTypeTransformByEs(dataFrame: DataFrame, sourceColumns: String, columnMap: String, ignoreCheck: Boolean = false, esMapping: JsonObject): DataFrame = {
  //    var columnTypeMap = Map[String, String]()
  //    sourceColumns.split(",").foreach(x => columnTypeMap += (x.split(":")(0) -> x.split(":")(1)))
  //
  //    var columnMapFinal = Map[String, String]()
  //    columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
  //
  //    // 判断是否需要处理，若无需处理，直接返回
  //    if (dataFrame.schema.count(x => complexTypeNames.contains(x.dataType.typeName)) > 0
  //      || dataFrame.schema.count(x => x.dataType.typeName.contains("decimal")) > 0
  //      || ignoreCheck) {
  //      val transColumns = dataFrame.schema.map(x =>
  //        if (complexTypeNames.contains(x.dataType.typeName)) {
  //          s"to_json(${x.name}) as ${columnMapFinal(x.name)}"
  //        } else if (x.dataType.typeName.toLowerCase().contains("decimal")) {
  //          s"cast(${x.name} as decimal(38,8)) as ${columnMapFinal(x.name)}"
  //        } else if (columnTypeMap(x.name).toLowerCase().contains("date") || columnTypeMap(x.name).toLowerCase().contains("timestamp")) {
  //          var esFormat = "yyyy-MM-dd"
  //          try {
  //            esFormat = esMapping.getAsJsonObject(x.name).get("format").toString.replaceAll("\"","")
  //          } catch {
  //            case e: Exception => println("获取es data format异常，使用默认format:yyyy-MM-dd")
  //          }
  //          s"date_format(${x.name}, '$esFormat') as ${columnMapFinal(x.name)}"
  //        } else {
  //          s"cast(${x.name} as ${columnTypeMap(x.name)}) as ${columnMapFinal(x.name)}"
  //        }
  //      )
  //      dataFrame.selectExpr(transColumns: _*)
  //    } else {
  //      dataFrame
  //    }
  //  }


  def fieldNameAndTypeTransformByEs(dataFrame: DataFrame, sourceColumns: String, columnMap: String, ignoreCheck: Boolean = false, esMapping: JsonObject, esJsonColumns: String): DataFrame = {
    // 判断是否需要处理，若无需处理，直接返回
    if (ignoreCheck) {
      var esJsonColumnsArray: Array[String] = null
      if (StringUtils.isNotBlank(esJsonColumns)) {
        esJsonColumnsArray = esJsonColumns.split(",")
      }
      val transColumns = dataFrame.schema.map(x =>
        //        if (x.dataType.typeName.toLowerCase().contains("date") || x.dataType.typeName.toLowerCase().contains("timestamp")) {
        if (esMapping.getAsJsonObject(x.name).get("type") != null && esMapping.getAsJsonObject(x.name).get("type").toString.replaceAll("\"", "").toLowerCase().contains("date")) {
          var esFormat = "yyyy-MM-dd"
          try {
            esFormat = esMapping.getAsJsonObject(x.name).get("format").toString.replaceAll("\"", "")
          } catch {
            case e: Exception => println("获取es data format异常，使用默认format:yyyy-MM-dd")
          }
          s"date_format(${x.name}, '$esFormat') as ${x.name}"
        } else if (esJsonColumnsArray != null && esJsonColumnsArray.nonEmpty && esJsonColumnsArray.contains(x.name)) {
          s"""CASE WHEN trim(${x.name}) LIKE '{%' THEN from_json(concat('[',${x.name},']'), 'array<map<string,string>>')
             |WHEN trim(${x.name}) LIKE '[%' THEN from_json(${x.name}, 'array<map<string,string>>')
             |END AS ${x.name}""".stripMargin
        } else if (x.dataType.typeName.toLowerCase().contains("decimal")) {
          s"cast(${x.name} as double) as ${x.name}"
        } else {
          s"cast(${x.name} as ${x.dataType.typeName}) as ${x.name}"
        }
      )
      dataFrame.selectExpr(transColumns: _*)
    } else {
      dataFrame
    }
  }

  //  def fieldNameAndTypeTransformByDeltaSink(deltaSourceDf: DataFrame,sinkDF: DataFrame): DataFrame = {
  //    val deltaSourceSchemaMap: Map[String, String] = deltaSourceDf.schema.map(field => (field.name, field.dataType.typeName)).toMap
  //
  //      val transColumns = sinkDF.schema.map(x =>
  //          s"cast(${x.name} as ${deltaSourceSchemaMap(x.name)}) as ${x.name}"
  //      )
  //    sinkDF.selectExpr(transColumns: _*)
  //    sinkDF.printSchema()
  //    sinkDF
  //  }

  /**
   * 字段名称，source和target映射,字段类型变更
   *
   * @param columnMap [[String]]
   * @return [[String]]
   */
  def hiveFieldNameAndTypeTransform(dataFrame: DataFrame, sourceColumns: String, columnMap: String, ignoreCheck: Boolean = false): DataFrame = {
    var columnTypeMap = Map[String, String]()
    sourceColumns.split(",").foreach(x => columnTypeMap += (x.split(":")(0) -> x.split(":")(1)))

    var columnMapFinal = Map[String, String]()
    columnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))

    // 判断是否需要处理，若无需处理，直接返回
    if (dataFrame.schema.count(x => complexTypeNames.contains(x.dataType.typeName)) > 0
      || dataFrame.schema.count(x => x.dataType.typeName.contains("decimal")) > 0
      || ignoreCheck) {
      val transColumns = dataFrame.schema.map(x =>
        if (complexTypeNames.contains(x.dataType.typeName)) {
          s"to_json(${x.name}) as ${columnMapFinal(x.name)}"
        } else if (x.dataType.typeName.toLowerCase().contains("decimal")) {
          s"cast(${x.name} as decimal(38,8)) as ${columnMapFinal(x.name)}"
        } else {
          s"cast(${x.name} as ${columnTypeMap(x.name)}) as ${columnMapFinal(x.name)}"
        }
      )
      dataFrame.selectExpr(transColumns: _*)
    } else {
      dataFrame
    }
  }
}