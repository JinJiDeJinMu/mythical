package com.hs.udfs.partitioner

trait TPartition {
  def getPartition(column: Long): String
}
