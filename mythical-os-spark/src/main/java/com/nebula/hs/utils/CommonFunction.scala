package com.nebula.hs.utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.collection.mutable

/**
 * @Description {{{
 *   scala常用function包装
 * }}}
 * @author wzw
 * @date 2023/2/7
 */
object CommonFunction {

  /**
   *
   * @param oldMap
   * @param updateMap
   * @return
   */
  def mapElementAdd(oldMap: Map[String, String]
                    , updateMap: Map[String, String]): Map[String, String] = {
    var newClause: Map[String, String] = oldMap
    updateMap
      .foreach(newClause += _)
    newClause
  }

  /**
   *
   * @param sourceMap
   * @param excludeMap
   * @return
   */
  def mapElementExclude(sourceMap: mutable.Map[String, String]
                        , excludeMap: mutable.Map[String, String]): mutable.Map[String, String] = {
    excludeMap.foreach(kv => {
      sourceMap -= kv._1
    })
    sourceMap
  }

  /**
   * 向kafka发送消息
   *
   * @param message [[String]]
   */
  def sendToKafka(message: String
                  , kafkaServers: String
                  , topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)
    producer.close()
  }
}
