package com.hs.utils.partition

import org.apache.spark.sql.Column

trait TPartition {
  def getPartition(sourceColumn: String
                   , targetColumn: String): Map[String, Column]
}
