package com.nebula.hs.plugins

import com.hs.common.SparkSessionWrapper
import com.hs.utils.DeltaMergeUtil.sortConfig
import com.hs.utils.FrameWriter
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util.Date

/**
 *
 * @Description
 * @author wzw
 * @date 2023/6/19
 */
object DeltaHistoryDataCount extends SparkSessionWrapper {
  override val appName: String = "CountHistoryDataVolume"
  //override val runMode: String = "local[*]"

  //var seqForDf: Seq[(String, BigInt, String)] = Seq()
  //var df:DataFrame =  spark.createDataFrame(seqForDf).toDF("dt","cnt","tb")
  val preUrl: String = "/user/hive/warehouse"

  def main(args: Array[String]): Unit = {
    val time_pre: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    // "stg,2008-01-01,2023-06-20"
    // 统计的层
    val targetDb = params.split(",")(0) //"stg"
    // 统计开始是时间
    //val startLine = params.split(",")(1)//2018-01-01
    // 统计截至时间
    val deadline = params.split(",")(1) //"2023-05-12"

    val path = preUrl + s"/${targetDb}.db"

    val filePath: Path = new org.apache.hadoop.fs.Path(path)
    val fileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files: Array[Path] = FileUtil.stat2Paths(fileSystem.listStatus(filePath))
    files.foreach(println)
    val subFileName: Array[String] = files.filter(fileSystem.getFileStatus(_).isDirectory()).map(_.getName)

    subFileName.foreach(subFile => {
      val sourcePath = path + "/" + subFile
      var tbFileSize: Long = 0
      //val cnt: Long =
      try {
        fileSystem.listStatus(new Path(sourcePath)).foreach(file => {
          tbFileSize += file.getLen
        })
        //        //spark.read.format("delta").load(sourcePath).where(s"insert_time <= '${deadline}'").count()
        //        val sourceDF = spark.read.format("delta").load(sourcePath)
        //
        //
        //        var resultDF = sourceDF
        //          .selectExpr("*","date_format(insert_time, 'yyyy-MM-dd') as dt")
        //          .where(s"date_format(insert_time, 'yyyy-MM-dd') <= '${deadline}'")
        //          .groupBy("dt")
        //          .count().withColumnRenamed("count","cnt")
        //          .selectExpr("dt", "cnt")
        var seqForDf: Seq[(String, Long)] = Seq()
        seqForDf = seqForDf :+ (subFile, tbFileSize / 1024 / 1024)
        val resultDF = spark.createDataFrame(seqForDf).toDF("tb", "storage")
        //resultDF = resultDF.withColumn("tb",lit(subFile))
        //df.union(resultDF)
        //df.show(false)
        save(resultDF)
        //resultDF.withColumn("tb",lit(subFile)).show(false)
      } catch {
        case e: FileNotFoundException => {
          println(e.getMessage)
          //0
        }
        case e: Exception => {
          println(e.getMessage)
          //0
        }
      }
      //totalFileSize += cnt
      /*
         val sourcePath = "/user/hive/warehouse/stg.db/stg_hs_dw_lget_aaalac_approve"
             spark.sql(
               s"""
                  |select date_format(insert_time, 'yyyy-MM-dd') as dt
                  |    ,count(*) as cnt
                  |from delta.`${sourcePath}`
                  |group by date_format(insert_time, 'yyyy-MM-dd')
                  |""".stripMargin).show(false)
     */
    })
    //df.show(false)
    //saveCsv(df)
    saveCsvRePat(s"/user/hive/warehouse/stg.db/delta_history_data_count_${time_pre}")
    //println(s"${targetDb} 层截至 ${deadline} 数据量为: " + totalFileSize + "条")
  }

  def save(dataFrame: DataFrame): Unit = {
    val time_pre: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    val writer = new FrameWriter(dataFrame, "append".toLowerCase, Option(null))
    writer.write(s"/user/hive/warehouse/stg.db/delta_history_data_count_${time_pre}", Option("dt"), sortConfig("append", "dt"))

    println("数据量:\n")
    dataFrame.show(false)
  }

  def saveCsvRePat(tablePath: String): Unit = {
    val time_pre: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    val sinkPath = s"/tmp/delta_history_data_count_${time_pre}"
    spark.read.format("delta")
      .load(tablePath)
      .repartition(1)
      .write
      .option("dataChange", "false")
      .format("csv")
      .mode("overwrite") //append
      .save(sinkPath)
  }

  def saveCsv(dataFrame: DataFrame): Unit = {
    val time_pre: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    dataFrame.write.option("header", value = true).mode(SaveMode.Append).csv(s"/tmp/delta_history_data_count_${time_pre}")
    println("数据量:\n")
    dataFrame.show(false)
  }
}
