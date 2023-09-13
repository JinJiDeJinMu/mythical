package com.hs.udfs.partitioner

object PartitionEnum extends Enumeration {
  type PartitionEnum = Value
  val DayPartition: Value = Value(0, "yyyy-MM-dd")
  val MonthPartition: Value = Value(1, "yyyy-MM")
  val YearPartition: Value = Value(2, "yyyy")
  val rangePartition: Value = Value(3, "")
}