package com.nebula.hs.etl.source

import com.hs.etl.config.EtlConfig
import com.hs.etl.config.source.MongoSourceConfig
import com.hs.utils.{OffsetUtil, Transformer}
import org.apache.spark.sql._

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
class MongoSource extends EtlSource[MongoSourceConfig] {

  override def getData(spark: SparkSession, jobConfig: EtlConfig): Dataset[Row] = {

    /**
     * 源数据集
     */
    val collection = config.getCollection

    /**
     * 增量插入字段: id,update_time
     */
    var offsetColumns = config.getOffsetColumns


    /** TODO:
     * 读取hdfs上的offset,拼接查询sql
     */
    val querySql = OffsetUtil.getQuerySqlByMongoSourceConfig(spark, config)
    println("querySql为：" + querySql)


    //    val map = new util.HashMap[String, String]()
    //    map.put("spark.mongodb.input.database", config.getSourceDatabase)  //数据库
    //    map.put("spark.mongodb.input.collection", collection)  //表
    //    map.put("spark.mongodb.input.uri", config.getUrl)  //uri
    //    map.put("readPreference.name", "secondaryPreferred")

    //    val readConfig = ReadConfig.create(map)
    //    var mdf: DataFrame = MongoSpark.load(spark, readConfig)

    val options = Map(
      "spark.mongodb.input.database" -> config.getSourceDatabase,
      "spark.mongodb.input.collection" -> collection,
      "spark.mongodb.input.uri" -> config.getUrl,
      "readPreference.name" -> "secondaryPreferred"
    )

    var mdf = spark.read.format("mongo").options(options).load()

    /*
        //解密 fixme: source table name gain
        Supplement.encryptDecryptTransMul(
          mutable.Map(" " -> mdf)
          ,"decrypt",jobConfig.getSource.getAlgConfig.asScala.toList)
    */
    mdf.createOrReplaceTempView(collection)
    mdf = Transformer.fieldNameAndTypeTransformByMongo(spark.sql(querySql), config.getSourceColumns, jobConfig.getColumnMap)

    if ("incr".equals(config.getReadMode)) {
      //获取source和target的字段映射关系
      //      var columnMapFinal = Map[String, String]()
      //      config.getColumnMap.split(",").foreach(x => columnMapFinal += (x.split(":")(0) -> x.split(":")(1)))
      //      offsetColumns = columnMapFinal(config.getOffsetColumns)
    }
    mdf
  }
}
