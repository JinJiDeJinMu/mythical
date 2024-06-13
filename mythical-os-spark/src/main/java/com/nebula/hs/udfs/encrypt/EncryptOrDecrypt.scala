package com.nebula.hs.udfs.encrypt

import com.hs.udfs.encrypt.EncryptKeyGenerator.generateKeySpe

import java.util.Base64
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, IllegalBlockSizeException}

/**
 * AES/SM4加密算法工具包
 * 模式:CBC
 * 填充方式:PKCS5Padding
 * 偏移量:16位
 *
 * @author wzw
 * @date 2023/3/16
 */
object EncryptOrDecrypt {
  //var ALGORITHM_NAME = "SM4"
  private val TRANSFORMATION = "%s/CBC/PKCS5Padding"
  private val IV_LENGTH = 16

  /**
   * 根据加密算法名称和密钥对指定的内容进行加密
   *
   * @param algorithmName [[String]]
   * @param key           [[String]]
   * @param plaintext     [[String]]
   * @return 密文  [[String]]
   */
  def encrypt(plaintext: String
              , algorithmName: String
              , key: String): String = {
    if (plaintext != null) { //"hs_null"
      val keyByt = key.getBytes("UTF-8")
      SecurityUtils.addBouncyCastleProvider()
      val cipher = Cipher.getInstance(TRANSFORMATION.format(algorithmName))
      val spec: SecretKeySpec = generateKeySpe(algorithmName, keyByt)
      //println(keyToString(spec))
      cipher.init(Cipher.ENCRYPT_MODE, spec, generateIv())
      val ciphertext = cipher.doFinal(plaintext.getBytes("UTF-8"))
      Base64.getEncoder.encodeToString(ciphertext)
    } else {
      //plaintext
      null
    }
  }

  /**
   * 偏移量
   *
   * @return
   */
  def generateIv(): IvParameterSpec = {
    val iv = new Array[Byte](IV_LENGTH)
    //util.Random.nextBytes(iv)
    new IvParameterSpec(iv)
  }

  /** TODO:密钥验证
   * 根据加密算法名称和密钥对指定的内容进行解密
   *
   * @param algorithmName [[String]]
   * @param key           [[String]]
   * @param ciphertext    [[String]]
   * @return 明文  [[String]]
   */
  def decrypt(ciphertext: String
              , algorithmName: String
              , key: String): String = {
    val keyByt = key.getBytes("UTF-8")
    if (ciphertext != null) { //"hs_null"
      val plaintext: String = try {
        SecurityUtils.addBouncyCastleProvider()
        val cipher = Cipher.getInstance(TRANSFORMATION.format(algorithmName))
        cipher.init(Cipher.DECRYPT_MODE, generateKeySpe(algorithmName, keyByt), generateIv())
        //cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(keyByt, algorithmName), new IvParameterSpec(new Array[Byte](16)))
        new String(cipher.doFinal(Base64.getDecoder.decode(ciphertext)), "UTF-8")
      } catch {
        case e: IllegalBlockSizeException => {
          e.printStackTrace()
          println(e.getMessage)
          ciphertext
        }
        case e: IllegalArgumentException => {
          e.printStackTrace()
          println(e.getMessage)
          ciphertext
        }
      }
      plaintext
    } else {
      null
    }
  }
}