package com.nebula.hs.plugins

import io.delta.tables._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import java.io.FileNotFoundException
import java.net.URI

object DeltaSmallFilesCompactionAndVacuum {

  val MB: Long = 1024 * 1024L
  val baseUrl: String = "/user/hive/warehouse"

  def main(args: Array[String]): Unit = {
    val hdfsIp = args(0)
    val hdfsClient = initFileSystem(hdfsIp, 8020)

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      //关闭检查
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .config("spark.databricks.delta.optimize.repartition.enabled", "true")
      //控制 vacuum 的并行度
      .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
      .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
      .appName("DeltaSmallFilesCompaction")
      //      .master("local[*]")
      .getOrCreate()

    val list = Array(
      baseUrl + "/stg.db",
      baseUrl + "/ods.db",
      baseUrl + "/dws.db",
      baseUrl + "/dwd.db",
      baseUrl + "/ads.db"
    )

    var cnt = 0
    list.foreach(path => {
      //    val path = "/user/hive/warehouse/stg.db"
      val filePath = new org.apache.hadoop.fs.Path(path)
      val fileSystem = filePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val allFiles = FileUtil.stat2Paths(fileSystem.listStatus(filePath))
      val res = allFiles.filter(fileSystem.getFileStatus(_).isDirectory()).map(_.getName.toString)

      var index = 0
      res.foreach(e => {
        val sourceTable = path + "/" + e

        if (DeltaTable.isDeltaTable(sourceTable)) {
          val deltaTable = DeltaTable.forPath(spark, sourceTable)

          try {
            println("clean table name : " + sourceTable)

            //            var deltaLog: FileStatus = hdfsClient.getFileStatus(new Path(sourceTable + "/_delta_log"))
            val dataFiles = hdfsClient.listStatus(new Path(sourceTable), (path: Path) => !path.getName.equals("_delta_log") && !path.getName.equals("_symlink_format_manifest"))
            println(s"--文件数为:${dataFiles.size}")

            if (dataFiles.size > 100 &&
              dataFiles.exists(dataFile => dataFile.getLen < 1024 * MB)) {

              println("--开始合并")
              //1. 合并小文件 逻辑清除
              deltaTable
                .optimize()
                .executeCompaction()

              index = index + 1
            }

          } catch {
            case ex: FileNotFoundException => println(ex.getMessage)
            case ex: Exception => println(ex.getMessage)
              println(s"----表可能正在被修改，跳过处理")
          } finally {
            //2. 物理清除,保留最新版本
            deltaTable.vacuum(24)
          }

          println(path + "聚合个数：" + index)
        }
      })
      cnt = cnt + index
    })

    println("聚合总个数：" + cnt)

    spark.stop()
  }

  private def initFileSystem(ip: String, port: Integer): FileSystem = {
    val configuration = new Configuration
    configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    FileSystem.get(new URI("hdfs://" + ip + ":" + port), configuration, "hdfs")
  }

}
