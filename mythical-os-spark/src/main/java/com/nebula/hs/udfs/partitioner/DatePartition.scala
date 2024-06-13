package com.nebula.hs.udfs.partitioner

import java.text.SimpleDateFormat
import java.util.TimeZone

class DatePartition(format: String) extends TPartition with Serializable {

  val sdf: SimpleDateFormat = new SimpleDateFormat(format)
  sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

  override def getPartition(column: Long): String = {
    var date: String = null
    var timestamp = column.toString
    if (timestamp.length >= 13) {
      date = sdf.format(timestamp.substring(0, 13).toLong)
    } else {
      while (timestamp.length < 13) {
        timestamp = timestamp + 0
      }
      date = sdf.format(timestamp.toLong)
    }
    date
  }
}
