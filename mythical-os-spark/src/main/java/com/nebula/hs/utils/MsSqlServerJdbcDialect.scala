package com.nebula.hs.utils

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType}

import java.util.Locale

/**
 * sqlserver jdbc dialect
 */
class MsSqlServerJdbcDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:sqlserver")

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case SpecificTypes.SQL_VARIANT => Some(StringType)
      case _ => None
    }
  }

  private object SpecificTypes extends Serializable {
    val SQL_VARIANT = -156
    //val GEOMETRY = -157
    //val GEOGRAPHY = -158
  }
}