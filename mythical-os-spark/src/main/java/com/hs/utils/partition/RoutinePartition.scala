package com.hs.utils.partition

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

class RoutinePartition(format: String) extends TPartition with Serializable {

  override def getPartition(sourceColumn: String
                            , targetColumn: String): Map[String, Column] = {
    var columnMap: Map[String, Column] = Map()
    columnMap += (
      targetColumn ->
        //trim(col(sourceColumn),format)
        col(sourceColumn)
      )
    columnMap
  }
}
