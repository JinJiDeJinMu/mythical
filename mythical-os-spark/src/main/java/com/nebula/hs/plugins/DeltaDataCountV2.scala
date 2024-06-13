package com.nebula.hs.plugins

import com.hs.common.SparkSessionWrapper
import com.hs.plugins.ConvertToDelta.DeltaV2xCompactionAndVacuum
import com.hs.utils.CommonFunction.sendToKafka
import com.hs.utils.DeltaMergeUtil.sortConfig
import com.hs.utils.FrameWriter
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions.{struct, to_json}
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
object DeltaDataCountV2 extends SparkSessionWrapper {
  override val appName: String = "CountDataVolumeV2"
  //override val runMode: String = "local[*]"

  override val updateConfig: Map[String, String] = Map(
    //关闭检查
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false"
    , "spark.databricks.delta.optimize.repartition.enabled" -> "true"
    , "spark.sql.broadcastTimeout" -> "36000"
    //控制 vacuum 的并行度
    , "spark.sql.sources.parallelPartitionDiscovery.parallelism" -> "4"
  )
  val preUrl: String = "/user/hive/warehouse"

  def main(args: Array[String]): Unit = {

    val time_pre: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    //saveToKafka("/delta/other/delta_history_data_count_20230821",params)

    val dbFilePath: Path = new org.apache.hadoop.fs.Path(preUrl)
    val dBFileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val dBFiles: Array[Path] = FileUtil.stat2Paths(dBFileSystem.listStatus(dbFilePath))
    dBFiles.foreach(println)
    val dbName: Array[String] = dBFiles.filter(dBFileSystem.getFileStatus(_).isDirectory()).filter(_.getName.contains(".db")).map(_.getName)
    dbName.foreach(println)

    dbName.foreach(dbFile => {
      val path = preUrl + s"/${dbFile}"

      val filePath: Path = new org.apache.hadoop.fs.Path(path)
      val fileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val files: Array[Path] = FileUtil.stat2Paths(fileSystem.listStatus(filePath))
      files.foreach(println)
      val subFileName: Array[String] = files.filter(fileSystem.getFileStatus(_).isDirectory()).map(_.getName)

      subFileName.foreach(subFile => {
        val sourcePath = path + "/" + subFile
        try {
          val sourceDF = spark.read.format("delta").load(sourcePath)

          val cnt_pre_time: String = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date)

          var seqForDf: Seq[(String, String, Long, String)] = Seq()
          seqForDf = seqForDf :+ (dbFile.substring(0, dbFile.length - 3), subFile, sourceDF.count(), cnt_pre_time)
          val resultDf = spark.createDataFrame(seqForDf).toDF("db", "tb", "cnt", "dt")
          save(resultDf)
        } catch {
          case e: FileNotFoundException => {
            println(e.getMessage)
          }
          case e: Exception => {
            println(e.getMessage)
          }
        }
      })
    })

    saveToKafka(s"/delta/other/delta_history_data_count_${time_pre}", params)
    saveCsvRePat(s"/delta/other/delta_history_data_count_${time_pre}")

    DeltaV2xCompactionAndVacuum(spark, s"/delta/other/delta_history_data_count_${time_pre}", 3)
  }

  def save(dataFrame: DataFrame): Unit = {
    val time_pre: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    val writer = new FrameWriter(dataFrame, "append".toLowerCase, Option(null))
    writer.write(s"/delta/other/delta_history_data_count_${time_pre}", Option("dt"), sortConfig("append", "dt"))

    println("数据量:\n")
    dataFrame.show(false)
  }

  def saveCsvRePat(tablePath: String): Unit = {
    val time_pre: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    val sinkPath = s"/delta/other/csv/delta_history_data_count_${time_pre}"
    spark.read.format("delta")
      .load(tablePath)
      .repartition(1)
      .write
      .option("dataChange", "false")
      .format("csv")
      .mode("overwrite") //append
      .save(sinkPath)
  }

  def saveToKafka(tablePath: String, kafkaInfo: String): Unit = {
    val stringBuilder = new StringBuilder("[")
    val df = spark.read.format("delta").load(tablePath).selectExpr("db as programId", "tb as code", "cnt as count", "dt")

    val sinkDf = df.select(to_json(struct(df.columns.head, df.columns.tail: _*))).as("value")

    //    df.write.format("kafka")
    //      .option("kafka.bootstrap.servers",kafkaInfo.split(",")(0))
    //      .option("topic",kafkaInfo.split(",")(1))
    //      .save()

    val message = sinkDf.collect().map(ele => ele.toString().drop(1).dropRight(1)).mkString(",")

    stringBuilder.append(message).append("]")


    println("数据量统计" + stringBuilder.toString())
    sendToKafka(stringBuilder.toString(), kafkaInfo.split(",")(0), kafkaInfo.split(",")(1))
  }

  def saveCsv(dataFrame: DataFrame): Unit = {
    val time_pre: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    dataFrame.write.option("header", value = true).mode(SaveMode.Append).csv(s"/tmp/delta_history_data_count_${time_pre}")
    println("数据量:\n")
    dataFrame.show(false)
  }
}
