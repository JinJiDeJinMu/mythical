package com.nebula.hs.utils

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType}

import java.util.Locale

/**
 * sqlserver jdbc dialect
 */
class KingbaseJdbcDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:kingbase8")


  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case java.sql.Types.OTHER => Some(StringType)
      case _ => None
    }
  }
}