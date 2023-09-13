package com.hs.udfs.partitioner

import com.hs.udfs.partitioner.PartitionEnum.{DayPartition, MonthPartition, YearPartition}
import org.apache.spark.sql.SparkSession


object PartitionFactory extends Enumeration {

  def Partition(partition: String): TPartition = {
    partition match {
      case "day" => new DatePartition(DayPartition.toString)
      case "month" => new DatePartition(MonthPartition.toString)
      case "year" => new DatePartition(YearPartition.toString)
      case _ => null
    }
  }

  def refresh(spark: SparkSession, database: String, table: String): Unit = {
    spark.sql("MSCK REPAIR TABLE " + database + "." + table + "_presto")
  }
}
