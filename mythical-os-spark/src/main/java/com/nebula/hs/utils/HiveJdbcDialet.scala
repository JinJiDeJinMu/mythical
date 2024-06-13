package com.nebula.hs.utils

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType}


/**
 * Hive jdbc dialect 类型映射
 */
class HiveJdbcDialet extends JdbcDialect {
  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:hive2")
  }

  override def quoteIdentifier(colName: String): String = {
    if (colName.contains(".")) {
      val newColName = colName.substring(colName.indexOf(".") + 1)
      return s"`$newColName`"
    }
    s"$colName"
  }

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case java.sql.Types.ARRAY => Some(StringType)
      case java.sql.Types.JAVA_OBJECT => Some(StringType)
      case _ => None
    }
  }
}
