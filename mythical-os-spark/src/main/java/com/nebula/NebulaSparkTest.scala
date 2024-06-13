package com.nebula

import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import org.apache.spark.sql.SparkSession

object NebulaSparkTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      //.config(sparkConf)
      .getOrCreate()

    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("192.168.201.57:9559")
      .withGraphAddress("192.168.201.57:9669")
      .build()


    //读取
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("hs-prod")
      .withLabel("delta")
      .withNoColumn(false)
      .withReturnCols(List("name","extra"))
      .withLimit(20)
      .withPartitionNum(10)
//      .withUser("root")
//      .withPasswd("nebula")
      .build()

    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()

    println(vertex)
  }
}
