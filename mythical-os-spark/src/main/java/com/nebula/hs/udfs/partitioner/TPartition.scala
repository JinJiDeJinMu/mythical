package com.nebula.hs.udfs.partitioner

trait TPartition {
  def getPartition(column: Long): String
}
