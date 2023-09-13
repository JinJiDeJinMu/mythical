/*
 *@Author   : DoubleTrey
 *@Time     : 2022/11/3 16:45
 */

package com.hs.common

import org.apache.spark.sql.types._

object DataType {

  def getType[T](originType: String): DataType = {
    originType.toLowerCase match {
      case "date" => DateType
      case "string" => StringType
      case "int" => IntegerType
      case "bigint" => LongType
      case "short" => ShortType
      case "float" => FloatType
      case "double" => DoubleType
      // TODO: 长度、精度指定
      case "decimal" => DecimalType(38, 8)
      case "bytetype" => ByteType
      case "boolean" => BooleanType
      case "timestamp" => TimestampType
      case "binary" => BinaryType
      case _ => throw new Exception(s"不支持该类型[$originType]")
    }
  }

}
