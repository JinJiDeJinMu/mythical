package com.nebula.hs.integration

import com.hs.common.SparkCommon
import com.hs.config.DeltaToStarRocksConfig
import com.hs.utils.Transformer.fieldNameTransform
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * delta table realtime to starrocks
 *
 * @author wzw
 * @date 2023/7/13
 */
object DeltaToStarRocks {
    val jobType = "%s-delta: %s.%s To sr: %s.%s"
    val deltaOptions: mutable.Map[String, String] = mutable.Map[String, String]()
    val starRocksOptions: mutable.Map[String, String] = mutable.Map[String, String](
        "sink.properties.format" -> "json"
        , "sink.properties.ignore_json_size" -> "true"
        , "sink.properties.strip_outer_array" -> "true"
    )

    def main(args: Array[String]): Unit = {
        if (args.length == 0) {
            throw new IllegalArgumentException("任务配置信息异常！")
        }

        val params = args(0)
        println("任务配置信息：" + params)

        // 1. job参数解析
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobConf = JsonMethods.parse(params).extract[DeltaToStarRocksConfig]
        println(jobConf)

        val sourceDbTb = jobConf.sourceDbTableMap.split(":")
        val sinkDbTb = jobConf.sinkDbTableMap.split(":")

        var ckPath = "/tmp/sr/%s-%s-%s".format(jobConf.taskID, sourceDbTb(0), sourceDbTb(1))
        // test path
        //ckPath = "/tmp/sr/sink/%s-%s".format(sinkDbTb(0),sinkDbTb(1))

        val sparkConfig = SparkCommon.sparkConfig
        sparkConfig.setIfMissing("spark.app.name", s"${jobType.format(jobConf.taskID, sourceDbTb(0), sourceDbTb(1), sinkDbTb(0), sinkDbTb(1))}")
        sparkConfig.setIfMissing("spark.sql.streaming.checkpointLocation", ckPath)

        // source option config
        /*
              deltaOptions += (
                "startingVersion" -> "1",
                "startingTimestamp" -> "2018-10-18"
              )
        */

        //source
        if (jobConf.isCDC == "true") {
            deltaOptions += ("readChangeFeed" -> "true")
        } else {
            deltaOptions += ("ignoreChanges" -> "true")
        }

        // sink option config
        starRocksOptions += (
          "starrocks.fe.http.url" -> jobConf.feHttp,
          "starrocks.fe.jdbc.url" -> jobConf.jdbcUrl,
          "starrocks.table.identifier" -> "%s.%s".format(sinkDbTb(0), sinkDbTb(1)),
          "starrocks.user" -> jobConf.username,
          "starrocks.password" -> jobConf.password,
          "checkpointLocation" -> ckPath
          //,"starrocks.write.label.prefix" -> label
        )

        if (jobConf.sinkColumn != null && jobConf.sinkColumn != "") {
            starRocksOptions += ("starrocks.columns" -> jobConf.sinkColumn)
        }

        val spark = SparkCommon.loadSession(sparkConfig)

        var sourcePath = jobConf.tablePathFormat.format(sourceDbTb(0), sourceDbTb(1))
        // test path
        sourcePath = "/tmp/delta/cdc/student2"
        val sourceDF = spark.readStream
          .format("delta")
          .options(deltaOptions)
          .load(sourcePath)


        val sinkDF = if (jobConf.sql != null && jobConf.sql != "") {
            sourceDF.createOrReplaceTempView(s"tmpTable_${jobConf.taskID}")
            spark.sql(jobConf.sql)
        } else if (jobConf.columnMap != null && jobConf.columnMap != "") {
            sourceDF.createOrReplaceTempView(s"tmpTable_${jobConf.taskID}")
            spark.sql(fieldNameTransform(jobConf.taskID, jobConf.columnMap))
        } else {
            sourceDF
        }


        //      sinkDF.writeStream.format("console").option("numRows",1000)
        //        .start()
        //        .awaitTermination()

        val writer: DataStreamWriter[Row] = sinkDF.writeStream.format("starrocks")
          .options(starRocksOptions)
          .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
          .outputMode(OutputMode.Append)

        val query = writer.start
        //query.stop()
        query.awaitTermination
    }
}
