package com.nebula.hs.etl.source

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.RedisSourceConfig
import com.hs.utils.{Supplement, Transformer}
import org.apache.spark.sql._

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class RedisSource extends EtlSource[RedisSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {

    /**
     * spark read Redis options
     */
    val readerOptions: Map[String, String] = Map(
      "host" -> config.getHost,
      "port" -> config.getPort,
      "dbNum" -> config.getRedisDb,
      "infer.schema" -> config.getInferSchema,
      "keys.pattern" -> config.getKeysPattern,
      "auth" -> config.getAuth,
    )

    val df = spark.read.format("org.apache.spark.sql.redis").options(readerOptions).load()

    df.createOrReplaceTempView("tmpTable_" + config.getTaskID)
    var sinkDf = spark.sql(Transformer.fieldNameTransformRedis(jobConfig.getColumnMap, config.getTaskID))

    /**
     * 系统字段填充
     */
    sinkDf = Supplement.systemFieldComplete(sinkDf)

    sinkDf
  }
}
