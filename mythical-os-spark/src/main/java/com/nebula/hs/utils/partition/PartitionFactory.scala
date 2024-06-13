package com.nebula.hs.utils.partition

object PartitionFactory {
  def Partition(partitionType: String, partitionFormat: String): TPartition = {

    partitionType match {
      case "time" => partitionFormat match {
        case "day" => new DatePartition("yyyy-MM-dd")
        case "month" => new DatePartition("yyyy-MM")
        case "year" => new DatePartition("yyyy")
        case _ => throw new IllegalArgumentException("不支持[%s]分区格式".format(partitionFormat))
      }
      case "type" => partitionFormat match {
        case "unChange" => new RoutinePartition(" ")
        case _ => throw new IllegalArgumentException("不支持[%s]分区格式,待设计扩展".format(partitionFormat))
      }
    }
  }
}
