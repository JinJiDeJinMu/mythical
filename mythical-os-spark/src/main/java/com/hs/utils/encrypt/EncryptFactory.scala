package com.hs.utils.encrypt

import com.hs.common.EncryptTypeConstant
import com.hs.common.EncryptTypeConstant.{AES, SM4}
import com.hs.config.{DecryptColConfig, EncryptColConfig}
import com.hs.etl.EncryptDecryptConfig
import com.hs.udfs.encrypt._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object EncryptFactory {
  var colType: String = _

  def Encrypt(dataFrame: DataFrame, encryptConfig: EncryptColConfig): DataFrame = {
    val columnName = encryptConfig.columnName
    val encryptType = encryptConfig.encryptName
    val encryptParams: List[Any] = encryptConfig.encryptParams
    dataFrame.schema.fields.foreach(x => {
      if (x.name == columnName) {
        colType = x.dataType.typeName
      }
    })
    // FIXME:字段空值问题
    encryptType match {
      case EncryptTypeConstant.AES => {
        val colName = encryptParams.head.asInstanceOf[String]
        val key = encryptParams(1).asInstanceOf[String]
        val AESEncryptUDF = udf(EncryptUDF.AESEncrypt _)
        dataFrame.withColumn(colName, AESEncryptUDF(col(colName), lit(key)))
      }
      case EncryptTypeConstant.SM4 => {
        val colName = encryptParams.head.asInstanceOf[String]
        val key = encryptParams(1).asInstanceOf[String]
        val SM4EncryptUDF = udf(EncryptUDF.SM4Encrypt _)
        dataFrame.withColumn(colName, SM4EncryptUDF(col(colName), lit(key)))
      }
      case EncryptTypeConstant.MD5 => {
        val salted = encryptParams.head.asInstanceOf[String]
        val colName = encryptParams(1).asInstanceOf[String]
        dataFrame.withColumn(colName, md5(concat_ws(salted, col(colName))))
      }
      case EncryptTypeConstant.NullInOrDel => {
        val colName = encryptParams.head.asInstanceOf[String]
        val deletionFlag = encryptParams(1).asInstanceOf[Boolean]
        val NullInOrDelEncryptUDF = udf(EncryptUDF.NullInOrDelEncrypt _)
        dataFrame.withColumn(colName, NullInOrDelEncryptUDF(col(colName), lit(deletionFlag)).cast(colType))
      }
      case EncryptTypeConstant.MaskWithChar => {
        val colName = encryptParams.head.asInstanceOf[String]
        val maskType = encryptParams(1).asInstanceOf[String]
        val start = encryptParams(2).toString.toInt
        val end = encryptParams(3).toString.toInt
        val MaskWithCharEncryptUDF = udf(EncryptUDF.MaskWithCharEncrypt _)
        dataFrame.withColumn(colName, MaskWithCharEncryptUDF(col(colName), lit(maskType), lit(start), lit(end)))
      }
      case EncryptTypeConstant.MaskWithRadChar => {
        val colName = encryptParams.head.asInstanceOf[String]
        val start = encryptParams(1).toString.toInt
        val end = encryptParams(2).toString.toInt
        val MaskWithRadCharEncryptUDF = udf(EncryptUDF.MaskWithRadCharEncrypt _)
        dataFrame.withColumn(colName, MaskWithRadCharEncryptUDF(col(colName), lit(start), lit(end)))
      }
      // FIXME:类型问题
      case EncryptTypeConstant.NumericalTrans => {
        val colName = encryptParams.head.asInstanceOf[String]
        val dataType = encryptParams(1).asInstanceOf[String]
        val left = encryptParams(2).toString.toInt
        val right = encryptParams(3).toString.toInt
        val unit = encryptParams(4).asInstanceOf[String]
        val NumericalTransEncryptUDF = udf(EncryptUDF.NumericalTransEncrypt _)
        dataFrame.withColumn(colName, NumericalTransEncryptUDF(col(colName), lit(dataType), lit(left), lit(right), lit(unit)).cast(colType))
      }
      case EncryptTypeConstant.Shuffle => {
        val colName = encryptParams.head.asInstanceOf[String]
        EncryptUDF.shuffleEncrypt(dataFrame, colName)
      }
      case _ => throw new IllegalArgumentException("不支持[%s]算法".format(encryptType))
    }
  }

  // new
  def Encrypt(dataFrame: DataFrame, encryptConfig: EncryptDecryptConfig): DataFrame = {
    val columnName = encryptConfig.getColName
    val encryptType = encryptConfig.getAlgName
    val encryptParams: List[Any] = encryptConfig.getAlgParams.asScala.toList
    dataFrame.schema.fields.foreach(x => {
      if (x.name == columnName) {
        colType = x.dataType.typeName
      }
    })
    // FIXME:字段空值问题
    encryptType match {
      case EncryptTypeConstant.AES => {
        val colName = encryptParams.head.asInstanceOf[String]
        val key = encryptParams(1).asInstanceOf[String]
        val AESEncryptUDF = udf(EncryptUDF.AESEncrypt _)
        dataFrame.withColumn(colName, AESEncryptUDF(col(colName), lit(key)))
      }
      case EncryptTypeConstant.SM4 => {
        val colName = encryptParams.head.asInstanceOf[String]
        val key = encryptParams(1).asInstanceOf[String]
        val SM4EncryptUDF = udf(EncryptUDF.SM4Encrypt _)
        dataFrame.withColumn(colName, SM4EncryptUDF(col(colName), lit(key)))
      }
      case EncryptTypeConstant.MD5 => {
        val salted = encryptParams.head.asInstanceOf[String]
        val colName = encryptParams(1).asInstanceOf[String]
        dataFrame.withColumn(colName, md5(concat_ws(salted, col(colName))))
      }
      case EncryptTypeConstant.NullInOrDel => {
        val colName = encryptParams.head.asInstanceOf[String]
        val deletionFlag = encryptParams(1).asInstanceOf[Boolean]
        val NullInOrDelEncryptUDF = udf(EncryptUDF.NullInOrDelEncrypt _)
        dataFrame.withColumn(colName, NullInOrDelEncryptUDF(col(colName), lit(deletionFlag)).cast(colType))
      }
      case EncryptTypeConstant.MaskWithChar => {
        val colName = encryptParams.head.asInstanceOf[String]
        val maskType = encryptParams(1).asInstanceOf[String]
        val start = encryptParams(2).toString.toInt
        val end = encryptParams(3).toString.toInt
        val MaskWithCharEncryptUDF = udf(EncryptUDF.MaskWithCharEncrypt _)
        dataFrame.withColumn(colName, MaskWithCharEncryptUDF(col(colName), lit(maskType), lit(start), lit(end)))
      }
      case EncryptTypeConstant.MaskWithRadChar => {
        val colName = encryptParams.head.asInstanceOf[String]
        val start = encryptParams(1).toString.toInt
        val end = encryptParams(2).toString.toInt
        val MaskWithRadCharEncryptUDF = udf(EncryptUDF.MaskWithRadCharEncrypt _)
        dataFrame.withColumn(colName, MaskWithRadCharEncryptUDF(col(colName), lit(start), lit(end)))
      }
      // FIXME:类型问题
      case EncryptTypeConstant.NumericalTrans => {
        val colName = encryptParams.head.asInstanceOf[String]
        val dataType = encryptParams(1).asInstanceOf[String]
        val left = encryptParams(2).toString.toInt
        val right = encryptParams(3).toString.toInt
        val unit = encryptParams(4).asInstanceOf[String]
        val NumericalTransEncryptUDF = udf(EncryptUDF.NumericalTransEncrypt _)
        dataFrame.withColumn(colName, NumericalTransEncryptUDF(col(colName), lit(dataType), lit(left), lit(right), lit(unit)).cast(colType))
      }
      case EncryptTypeConstant.Shuffle => {
        val colName = encryptParams.head.asInstanceOf[String]
        EncryptUDF.shuffleEncrypt(dataFrame, colName)
      }
      case _ => throw new IllegalArgumentException("不支持[%s]算法".format(encryptType))
    }
  }

  def Decrypt(dataFrame: DataFrame, decryptConfig: DecryptColConfig): DataFrame = {
    val columnName = decryptConfig.columnName
    val decryptType = decryptConfig.decryptName
    val decryptParams: List[Any] = decryptConfig.decryptParams
    decryptParams.foreach(println)
    dataFrame.schema.fields.foreach(x => {
      if (x.name == columnName) {
        colType = x.dataType.typeName
      }
    })
    decryptType match {
      case EncryptTypeConstant.AES => {
        val colName = decryptParams.head.asInstanceOf[String]
        val key = decryptParams(1).asInstanceOf[String]
        val AESDecryptUDF = udf(DecryptUDF.AESDecrypt _)
        dataFrame.withColumn(colName, AESDecryptUDF(col(colName), lit(key)))
      }
      case EncryptTypeConstant.SM4 => {
        val colName = decryptParams.head.asInstanceOf[String]
        val key = decryptParams(1).asInstanceOf[String]
        val SM4DecryptUDF = udf(DecryptUDF.SM4Decrypt _)
        dataFrame.withColumn(colName, SM4DecryptUDF(col(colName), lit(key)))
      }
      case _ => throw new IllegalArgumentException("不支持[%s]不可逆算法".format(decryptType))
    }
  }

  // new
  def Decrypt(dataFrame: DataFrame, decryptConfig: EncryptDecryptConfig): DataFrame = {
    val columnName = decryptConfig.getColName
    val decryptType = decryptConfig.getAlgName
    val decryptParams: List[Any] = decryptConfig.getAlgParams.asScala.toList
    decryptParams.foreach(println)
    dataFrame.schema.fields.foreach(x => {
      if (x.name == columnName) {
        colType = x.dataType.typeName
      }
    })
    decryptType match {
      case EncryptTypeConstant.AES => {
        val colName = decryptParams.head.asInstanceOf[String]
        val key = decryptParams(1).asInstanceOf[String]
        val AESDecryptUDF = udf(DecryptUDF.AESDecrypt _)
        dataFrame.withColumn(colName, AESDecryptUDF(col(colName), lit(key)))
      }
      case EncryptTypeConstant.SM4 => {
        val colName = decryptParams.head.asInstanceOf[String]
        val key = decryptParams(1).asInstanceOf[String]
        val SM4DecryptUDF = udf(DecryptUDF.SM4Decrypt _)
        dataFrame.withColumn(colName, SM4DecryptUDF(col(colName), lit(key)))
      }
      case _ => throw new IllegalArgumentException("不支持[%s]不可逆算法".format(decryptType))
    }
  }
}

// 加密
object EncryptUDF {
  /**
   * AES
   *
   * @param plaintext [[String]]
   * @param key       [[String]]
   * @return
   */
  def AESEncrypt(plaintext: String, key: String): String = {
    EncryptOrDecrypt.encrypt(plaintext, AES, key)
  }

  /**
   * SM4
   *
   * @param plaintext [[String]]
   * @param key       [[String]]
   * @return
   */
  def SM4Encrypt(plaintext: String, key: String): String = {
    EncryptOrDecrypt.encrypt(plaintext, SM4, key)
  }

  /**
   * 掩码
   *
   * @param plaintext [[String]]
   * @param maskType  [[String]]
   * @param start     [[Int]]
   * @param end       [[Int]]
   * @return
   */
  def MaskWithCharEncrypt(plaintext: String
                          , maskType: String = "*"
                          , start: Int = 0
                          , end: Int = 0): String = {
    MaskWithChar.encrypt(plaintext, maskType, start, end)
  }

  /**
   * 字符替换
   *
   * @param plaintext [[String]]
   * @param start     [[Int]]
   * @param end       [[Int]]
   * @return
   */
  def MaskWithRadCharEncrypt(plaintext: String
                             , start: Int
                             , end: Int): String = {
    MaskWithRadChar.encrypt(plaintext, start, end)
  }

  /**
   * 数值变换
   *
   * @param plaintext [[String]]
   * @param left      [[Int]]
   * @param right     [[Int]]
   * @param unit      [[String]]
   * @return
   */
  def NumericalTransEncrypt(plaintext: Any
                            , dataType: String
                            , left: Int
                            , right: Int
                            , unit: String): String = {
    NumericalTrans.encrypt(plaintext, dataType, left, right, unit)
  }


  /**
   * 空值插入/替换
   *
   * @param plaintext    [[String]]
   * @param deletionFlag [[Boolean]]
   * @return
   */
  def NullInOrDelEncrypt(plaintext: String
                         , deletionFlag: Boolean): String = {
    NullInOrDel.encrypt(plaintext, deletionFlag)
  }

  /**
   * 混洗
   *
   * @param df      [[DataFrame]]
   * @param colName [[String]]
   * @return
   */
  def shuffleEncrypt(df: DataFrame
                     , colName: String): DataFrame = {
    ShuffleData.encrypt(df, colName)
  }
}

// 解密
object DecryptUDF {
  /**
   * AES
   *
   * @param ciphertext [[String]]
   * @param key        [[String]]
   * @return
   */
  def AESDecrypt(ciphertext: String, key: String): String = {
    EncryptOrDecrypt.decrypt(ciphertext, AES, key)
  }

  /**
   * SM4
   *
   * @param ciphertext [[String]]
   * @param key        [[String]]
   * @return
   */
  def SM4Decrypt(ciphertext: String, key: String): String = {
    EncryptOrDecrypt.decrypt(ciphertext, SM4, key)
  }
}