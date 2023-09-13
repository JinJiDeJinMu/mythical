package com.hs.plugins

import com.hs.common.SparkSessionWrapper
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

import scala.collection.mutable

/**
 * @Description 将普通的parquet文件转换未delta表
 * @author wzw
 * @date 2023/1/10
 */
object ConvertToDelta extends SparkSessionWrapper {
  override val appName: String = "parquetConvertToDelta"
  /* FIXME: 集群运行记得注释该行代码 */
  override val runMode: String = "local[*]"
  /* 任务定制化config */
  override val updateConfig: Map[String, String] = Map(
    //关闭检查
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false"
    , "spark.databricks.delta.optimize.repartition.enabled" -> "true"
    , "spark.sql.broadcastTimeout" -> "36000"
    //控制 vacuum 的并行度
    //, "spark.sql.sources.parallelPartitionDiscovery.parallelism" -> "4"
  )
  val tablePathFormat: String = "/user/hive/warehouse/%s.db/%s"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }
    val params = args(0)
    println("需修复表信息：" + params)

    /**
     * One-button repair
     */
    println(OneButtonRepair(FileSystem.get(spark.sparkContext.hadoopConfiguration), args(0)))

  }

  /**
   * One-button repair!
   *
   * @param fileSystem
   * @param dbTbMap [[String]] -> eg.{ods:ods_table1,dwd:dwd_table2}
   */
  def OneButtonRepair(fileSystem: FileSystem
                      , dbTbMap: String): String = {
    GenerateDeltaTablePath(dbTbMap).foreach(kv => {
      val sourceTablePath = kv._2
      if (IsOrNotDeltaTable(spark, sourceTablePath)) {
        /** *********************************************************************************************************** */
        //  1. 【停止任务】

        //  2. 【合并小文件,清除保留最新版本】
        //DeltaV2xCompactionAndVacuum(spark,sourceTable,3)

        //  3. 【删除目录下除parquet的其他文件】
        /* 【将delta表转换为普通的parquet表】
              // a. 删除不属于最新版本的所有数据文件
                val deltaTable1: DeltaTable = DeltaTableForPath(spark,sourceTable)
                deltaTable1.vacuum(0)
              // b. 删除目录下除parquet的其他文件
                hadoop fs -rm -r /user/hive/warehouse/stg.db/stg_crawler_device_incr_dt/_delta_log
                hadoop fs -rm -r /user/hive/warehouse/stg.db/stg_crawler_device_incr_dt/_symlink_format_manifest
                最后留下的就是普通的parquet文件
        */
        val logDeleteOrElse = fileSystem.delete(new Path(sourceTablePath + "/_delta_log"), true)
        val manifestDeleteOrElse = fileSystem.delete(new Path(sourceTablePath + "/_symlink_format_manifest"), true)
        println(s"1. 表${kv._1}路径下: 目录 _delta_log 删除成功否-> ${logDeleteOrElse}  目录 symlink_format_manifest 删除成功否 -> ${manifestDeleteOrElse}")

        //  4. 【转换为Delta表】
        ConvertToDelta(spark, sourceTablePath)
        println(s"2. 表${kv._1}转换成功！")

        //  5. 【生成清单文件,并开启自动更新】
        GenerateCommand(spark, sourceTablePath)
        println(s"3. 表${kv._1}清单文件生成成功,已开启自动更新！")

        //  6. 【查看表的属性是否更改成功】
        //ScanTableProperties(spark,sourceTable)

        //  7. 【查看转换是否成功】
        //DeltaTableForPath(spark,sourceTable).toDF.show(false)

        //  8.  【合并小文件】
        DeltaV2xCompactionAndVacuum(spark, sourceTablePath, 3)

        /** *********************************************************************************************************** */
      } else {
        println(s"表${sourceTablePath}不存在,跳过处理")
      }
    })
    "修复完成！"
  }

  /**
   * 判断是否为delta表
   *
   * @param spark       [[SparkSession]]
   * @param sourceTable [[String]]
   * @return
   */
  def IsOrNotDeltaTable(spark: SparkSession
                        , sourceTable: String): Boolean = {
    DeltaLog.forTable(spark, sourceTable).tableExists
  }

  /**
   * generate delta table's path
   *
   * @param dbTableMap [[String]] -> eg.{ods:ods_company_info}
   * @return
   */
  def GenerateDeltaTablePath(dbTableMap: String): mutable.Map[String, String] = {
    val tarTablePathMap = mutable.Map[String, String]()
    dbTableMap.split(",").foreach(source => {
      val split = source.split(":")
      tarTablePathMap += (
        split(1) ->
          tablePathFormat.format(split(0), split(1))
        )
    })
    tarTablePathMap
  }

  /**
   * 生成delta表的清单文件,并开启自动更新
   *
   * @param spark
   * @param tablePath
   */
  def GenerateCommand(spark: SparkSession, tablePath: String): Unit = {
    // 开启清单文件自动更新
    spark.sql(s"ALTER TABLE delta.`${tablePath}` SET TBLPROPERTIES ('delta.compatibility.symlinkFormatManifest.enabled' = 'true')")
    // 生成清单文件
    val deltaTable: DeltaTable = DeltaTableForPath(spark, tablePath)
    deltaTable.generate("symlink_format_manifest")
  }

  /**
   * 普通parquet文件转换为delta表
   *
   * @param spark
   * @param tablePath
   */
  def ConvertToDelta(spark: SparkSession, tablePath: String): Unit = {
    /**
     * 目录下的parquet文件转换为delta表
     */
    // 转换非分区的 parquet table at path '/path/to/table'
    val deltaTable: DeltaTable = DeltaTable
      .convertToDelta(spark, s"parquet.`${tablePath}`")
    deltaTable.toDF.printSchema()
  }

  /**
   * 自动根据磁盘上的大小生成均匀平衡的数据文件
   *
   * @param spark
   * @param tablePath
   */
  def DeltaV2xCompactionAndVacuum(spark: SparkSession, tablePath: String, inter: Int): Unit = {
    // 修改日志保留vacuum后日志保留时间
    AlterTableProperties(spark, tablePath, inter)

    val deltaTable = DeltaTableForPath(spark, tablePath)
    //1. 合并小文件 逻辑清除
    deltaTable
      .optimize()
      .executeCompaction()
    println("Compaction 成功！")

    //2. 物理清除,保留最新版本
    deltaTable.vacuum(0)
    println("vacuum 成功！")
  }

  /**
   * read DeltaTable
   *
   * @param sourceTable
   * @return
   */
  def DeltaTableForPath(spark: SparkSession, sourceTable: String): DeltaTable = {
    DeltaTable.forPath(spark, sourceTable)
  }

  /**
   * 控制文件在成为 VACUUM 候选之前必须删除多长时间 (默认7天)
   *
   * @param spark
   * @param sourceTable
   * @param inter
   */
  def AlterTableProperties(spark: SparkSession, sourceTable: String, inter: Int): Unit = {
    spark.sql(s"ALTER TABLE delta.`${sourceTable}` SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval ${inter} days')")
  }

  /**
   * 查看表指定表有哪表属性
   *
   * @param spark
   * @param sourceTable
   */
  def ScanTableProperties(spark: SparkSession, sourceTable: String): Unit = {
    spark.sql(s"SHOW TBLPROPERTIES delta.`${sourceTable}`").show(false)
  }
}

