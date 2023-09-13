package com.hs.udfs.encrypt

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import scala.math.BigDecimal.javaBigDecimal2bigDecimal

/**
 *
 * @Description
 * @author wzw
 * @date 2023/4/17
 */
object NumericalTrans {
  val timeTypeNames: Array[String] = Array("Timestamp", "Date")
  val numericTypeNames: Array[String] = Array("Integer", "Long", "Short", "Float", "Double", "BigDecimal")

  /** fixme:入参为Double类型,其它数值类型是否兼容?
   * 数值类型脱敏
   * ··根据给定范围随机百分比对原始值进行增减
   *
   * @param value [[Double]] 明文
   * @param m     [[Double]]
   * @param n     [[Double]]
   * @return
   */
  def transformNumericData1(value: Double
                            , m: Double
                            , n: Double
                            , unit: String): String = {
    val randomPercentage = (scala.util.Random.nextDouble() * (n - m) + m) / 100.0
    //fixme:精度控制后变为String类型
    "%.4f".format(value * (1.0 + randomPercentage))
  }

  /**
   * 数值变换
   *
   * @param value [[Any]]
   * @param left  [[Int]]
   * @param right [[Int]]
   * @param unit  [[String]]
   * @return
   */
  def encrypt(value: Any
              , dataType: String
              , left: Int
              , right: Int
              , unit: String): String = {
    if (value != null) {
      val valueTypeName = value.getClass.getSimpleName
      // println(valueTypeName)
      if (timeTypeNames.contains(valueTypeName) || dataType == "time") {
        transformTimestampData(value, left, right, unit).toString
        /*
              val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val str = sdf.format(timestamp)
              str
        */
      } else if (numericTypeNames.contains(valueTypeName) || dataType == "num") {
        transformNumericData(value, left, right, unit).toString
        //"%.4f".format(transformNumericData(value,left,right,unit))
      } else {
        value.toString
      }
    } else {
      //value.toString
      null
    }
  }

  /**
   * 数值类型脱敏
   * ··根据给定范围随机百分比对原始值进行增减
   *
   * @param value [[Any]] 明文
   * @param m     [[Double]]
   * @param n     [[Double]]
   * @return
   */
  def transformNumericData(value: Any
                           , m: Double
                           , n: Double
                           , unit: String): Double = {
    val randomPercentage = (scala.util.Random.nextDouble() * (n - m) + m) / 100.0
    if (value.getClass.getSimpleName == "int"
      || value.getClass.getSimpleName == "Integer") {
      (value.asInstanceOf[Int] * (1.0 + randomPercentage)) //.toInt
    } else if (value.getClass.getSimpleName == "long"
      || value.getClass.getSimpleName == "Long") {
      (value.asInstanceOf[Long] * (1.0 + randomPercentage)) //.toLong
    } else if (value.getClass.getSimpleName == "short"
      || value.getClass.getSimpleName == "Short") {
      (value.asInstanceOf[Short] * (1.0 + randomPercentage)) //.toShort
    } else if (value.getClass.getSimpleName == "float"
      || value.getClass.getSimpleName == "Float") {
      (value.asInstanceOf[Float] * (1.0 + randomPercentage)) //.toFloat
    } else if (value.getClass.getSimpleName == "BigDecimal"
      || value.getClass.getSimpleName == "decimal") {
      (value.asInstanceOf[java.math.BigDecimal].toDouble * (1.0 + randomPercentage))
    } else if (value.getClass.getSimpleName == "double"
      || value.getClass.getSimpleName == "Double") {
      value.asInstanceOf[Double] * (1.0 + randomPercentage)
    } else if (value.getClass.getSimpleName == "string"
      || value.getClass.getSimpleName == "String") {
      value.asInstanceOf[String].toInt * (1.0 + randomPercentage)
    } else {
      value.asInstanceOf[Double] * (1.0 + randomPercentage)
    }
  }

  /** fixme:兼容Date类型
   * 时间类型脱敏
   * ··根据给定范围随机值对原始值进行增减
   *
   * @param value [[Timestamp]]
   * @param m     [[Int]]
   * @param n     [[Int]]
   * @param unit  [[String]]
   * @return
   */
  def transformTimestampData(value: Any
                             , m: Int
                             , n: Int
                             , unit: String): Timestamp = {
    val randomOffset = scala.util.Random.nextInt(n - m + 1) + m
    val cal = java.util.Calendar.getInstance()
    if (value.getClass.getSimpleName == "Date"
      || value.getClass.getSimpleName == "date") { //.asInstanceOf[Timestamp]
      cal.setTimeInMillis(new Timestamp(value.asInstanceOf[Date].getTime).getTime)
    } else if (value.getClass.getSimpleName == "string"
      || value.getClass.getSimpleName == "String") {
      var sdf = new SimpleDateFormat("")
      val timeFormat = convertToDateTime(value.toString)
      if (timeFormat == "TimestampL") {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      } else if (timeFormat == "Timestamp") {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      } else if (timeFormat == "Date") {
        sdf = new SimpleDateFormat("yyyy-MM-dd")
      } else {
        throw new Exception("Invalid format")
      }
      cal.setTimeInMillis(new Timestamp(sdf.parse(value.asInstanceOf[String]).getTime).getTime)
    } else {
      cal.setTimeInMillis(value.asInstanceOf[Timestamp].getTime)
    }
    unit match {
      case "year" => cal.add(java.util.Calendar.YEAR, randomOffset)
      case "month" => cal.add(java.util.Calendar.MONTH, randomOffset)
      case "day" => cal.add(java.util.Calendar.DAY_OF_YEAR, randomOffset)
      case "hour" => cal.add(java.util.Calendar.HOUR_OF_DAY, randomOffset)
      case "minute" => cal.add(java.util.Calendar.MINUTE, randomOffset)
      case "second" => cal.add(java.util.Calendar.SECOND, randomOffset)
    }
    new java.sql.Timestamp(cal.getTimeInMillis)
  }

  /**
   * 时间格式判断
   *
   * @param str
   * @return
   */
  def convertToDateTime(str: String): String = { //Option[Any]
    val datePattern = "yyyy-MM-dd"
    val timestampPattern = "yyyy-MM-dd HH:mm:ss"
    val timestampPattern1 = "yyyy-MM-dd HH:mm:ss.SSS"
    val dateFormatter = new SimpleDateFormat(datePattern)
    val timestampFormatter = new SimpleDateFormat(timestampPattern)
    val timestampFormatter1 = new SimpleDateFormat(timestampPattern1)

    try {
      val timestamp1: Timestamp = new Timestamp(timestampFormatter1.parse(str).getTime)
      "TimestampL"
      //Some(timestamp1)
    } catch {
      case _: Exception => {
        try {
          val timestamp: Timestamp = new Timestamp(timestampFormatter.parse(str).getTime)
          //Some(timestamp)
          "Timestamp"
        } catch {
          case _: Exception => {
            try {
              val date: Date = new Date(dateFormatter.parse(str).getTime)
              //Some(date)
              "Date"
            } catch {
              case _: Exception => "Invalid format" //None
            }
          }
        }
      }
    }
  }
}
