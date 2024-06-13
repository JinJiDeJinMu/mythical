package com.nebula.hs.udfs.encrypt

import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.{SecureRandom, Security}
import java.util.Base64
import javax.crypto.KeyGenerator
import javax.crypto.spec.SecretKeySpec


/**
 * AES/SM4加密算法密钥生成工具
 *
 * @author wzw
 * @date 2023/3/16
 */
object EncryptKeyGenerator {
  //private var ALGORITHM_NAME = "SM4" //AES
  private val KEY_LENGTH = 16

  /**
   * 密钥可读
   *
   * @param key [[String]]
   * @return 明钥  [[String]]
   */
  def keyToString(key: SecretKeySpec): String = {
    new String(key.getEncoded, "UTF-8")
  }

  /**
   * 根据算法名和密钥生成对应的密钥规格
   *
   * @param algorithmName [[String]]
   * @return [[SecretKeySpec]]
   */
  def generateKeySpe(algorithmName: String, key: Array[Byte]): SecretKeySpec = {
    /*
        val paddedKey = if (key.length < KEY_LENGTH) {
          val padded = new Array[Byte](KEY_LENGTH)
          System.arraycopy(key, 0, padded, 0, key.length)
          padded
        } else if (key.length > KEY_LENGTH) {
          val truncated = new Array[Byte](KEY_LENGTH)
          System.arraycopy(key, 0, truncated, 0, KEY_LENGTH)
          truncated
        } else {
          key
        }
    */

    new SecretKeySpec(key, algorithmName)
  }

  /**
   * 根据算法名称生成公私钥(SM4/AES)
   *
   * @param EncryptionMode [[String]]
   * @return [[String]]
   */
  def generateKey(EncryptionMode: String): String = {
    SecurityUtils.addBouncyCastleProvider()
    //密钥长度
    val keyLength = 128
    val keyGen = KeyGenerator.getInstance(EncryptionMode)
    //val secureRandom: SecureRandom = SecureRandom.getInstanceStrong
    val secureRandom: SecureRandom = new SecureRandom()
    keyGen.init(keyLength, secureRandom)
    // 生成 AES 密钥
    val key = keyGen.generateKey()
    var keyString = Base64.getEncoder.encodeToString(key.getEncoded)
    // 将二进制密钥编码为十六进制字符串
    //keyString = DatatypeConverter.printHexBinary(key.getEncoded())
    keyString
  }
}

object SecurityUtils {
  def addBouncyCastleProvider(): Unit = {
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider())
    }
  }
}