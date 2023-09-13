package com.hs.utils

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType}

import java.util.Locale

/**
 * sqlserver jdbc dialect
 */
class DmJdbcDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:dm")


  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case java.sql.Types.OTHER => Some(StringType)
      case _ => None
    }
  }
}