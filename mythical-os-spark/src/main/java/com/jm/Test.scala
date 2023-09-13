package com.jm

import com.jm.common.SparkCommon
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    val sparkConfig = SparkCommon.sparkConfig
    val spark: SparkSession = SparkCommon.loadSession(sparkConfig)

    val dataFrame = spark.read.format("delta")
      .load("hdfs://192.168.110.42:8020/user/hive/warehouse/dwd.db/company_info_2")

    dataFrame.show()
  }

}
