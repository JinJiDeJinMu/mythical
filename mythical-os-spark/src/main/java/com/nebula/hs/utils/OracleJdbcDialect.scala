package com.nebula.hs.utils

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._

import java.util.Locale

/**
 * sqlserver jdbc dialect
 */
class OracleJdbcDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:oracle")

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case SpecificTypes.BFILE => Some(StringType)
      //      case SpecificTypes.INTERVAL_DAY_TO_SECOND => Some(DayTimeIntervalType(DAY,SECOND))
      //      case SpecificTypes.INTERVAL_YEAR_TO_MONTH => Some(YearMonthIntervalType(YEAR,MONTH))
      case SpecificTypes.INTERVAL_DAY_TO_SECOND => Some(StringType)
      case SpecificTypes.INTERVAL_YEAR_TO_MONTH => Some(StringType)
      case SpecificTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE => Some(TimestampType)
      //      case SpecificTypes.BLOB => Some(StringType)
      //      case SpecificTypes.CLOB => Some(StringType)
      case _ => None
    }
  }

  private object SpecificTypes extends Serializable {
    val BFILE = -13
    val INTERVAL_DAY_TO_SECOND = -104
    val INTERVAL_YEAR_TO_MONTH = -103
    val TIMESTAMP_WITH_LOCAL_TIME_ZONE = -102
    //    val BLOB = 2004
    //    val CLOB = 2005
  }
}