package com.hs.utils

import org.apache.commons.net.ftp.{FTPClient, FTPClientConfig, FTPReply}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.nio.charset.Charset
import scala.collection.mutable.ListBuffer

class FtpUtil {

  /**
   * 构建schema信息
   *
   * @param column
   * @return
   */
  def createSchema(columnArr: Array[String]) = {
    var columns = ""
    val structFieldList = new ListBuffer[StructField]()

    for (i <- 0 until columnArr.length) {
      structFieldList += StructField(columnArr(i).toLowerCase, StringType, true)
      if (columnArr(i).toLowerCase == "type") {
        columns += ("," + "`" + columnArr(i).toLowerCase + "`")
      } else {
        columns += ("," + columnArr(i).toLowerCase)
      }
    }
    val schema = StructType(structFieldList)
    structFieldList.clear()
    (schema, columns, columnArr.length)
  }


  /**
   * connect
   *
   * @param url
   * @param port
   * @param username
   * @param password
   * @return
   */
  def connectFtp(url: String, port: Int, username: String, password: String): FTPClient = {
    var connectUrl = url
    val proto = "ftp://"
    if (url.startsWith(proto)) {
      connectUrl = url.substring(6, url.length)
    }
    val ftpClient = new FTPClient()
    ftpClient.connect(connectUrl)
    ftpClient.login(username, password)

    ftpClient.setControlEncoding(Charset.defaultCharset().name())
    val replyCode = ftpClient.getReplyCode
    if (!FTPReply.isPositiveCompletion(replyCode)) {
      throw new RuntimeException("replyCode:" + replyCode)
    }
    // 获取登录信息
    val config = new FTPClientConfig(ftpClient.getSystemType.split(" ")(0))
    config.setServerLanguageCode("zh")
    ftpClient.configure(config)
    ftpClient.enterLocalPassiveMode()

    ftpClient
  }


  def createDefaultHeadColumn(num: Int): Array[String] = {
    val ss: Array[String] = Array()
    for (i <- 0 to num - 1) {
      ss :+ "column" + i + 1
    }
    ss
  }
}
