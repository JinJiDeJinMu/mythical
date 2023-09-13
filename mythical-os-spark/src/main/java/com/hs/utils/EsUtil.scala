//package com.hs.utils
//
//import com.google.gson.JsonObject
//import io.searchbox.indices.mapping.GetMapping
//import org.apache.http.HttpHost
//import org.apache.spark.internal.config
//
//
//class EsUtil {
//  def getEsMapping(hostname: String,port:Int,username:String,password:String, esIndex:String): JsonObject = {
//    val esClient: ESClient = new ESClient
//    esClient.createClient(new HttpHost(hostname, port).toURI, username, password, false, 300000, false, false)
//
//    //    val getMapping = new GetMapping.Builder().addIndex(s"${config.getEsIndex}/${config.getEsType}").build
//    val getMapping = new GetMapping.Builder().addIndex(esIndex).build
//    val jsonObject: JsonObject = esClient.execute(getMapping).getJsonObject
//    jsonObject.getAsJsonObject(esIndex).getAsJsonObject("mappings").getAsJsonObject("properties")
//  }
//}
