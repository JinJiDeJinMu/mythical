package com.nebula.hs.utils.partition

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, date_format, to_date}

class DatePartition(format: String) extends TPartition with Serializable {

  override def getPartition(sourceColumn: String
                            , targetColumn: String): Map[String, Column] = {
    var columnMap: Map[String, Column] = Map()
    columnMap += (
      targetColumn ->
        date_format(to_date(col(sourceColumn)), format)
      )
    //trim(col(sourceColumn))
    columnMap
  }
}
