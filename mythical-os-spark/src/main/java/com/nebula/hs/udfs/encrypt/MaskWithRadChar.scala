package com.nebula.hs.udfs.encrypt

import scala.util.Random

/**
 * 字符替换：
 * 位置：前三位/后四位/全部替换
 * 方式：字母替换为随机字母,数字替换为随机数字,其它不变
 *
 * @author wzw
 * @date 2023/4/17
 */
object MaskWithRadChar {
  def encrypt(str: String
              , start: Int
              , end: Int): String = {
    if (str != null) {
      val random = new Random()
      // 前三
      if (start != 0 & end == 0) {
        if (str.length >= 3) {
          val prefix = str.substring(0, 3).map(c => {
            if (String.valueOf(c).matches("[\u4e00-\u9fa5]")) {
              c
            } else if (c.isDigit) {
              random.nextInt(10).toString.charAt(0)
            } else if (c.isLetter) {
              val randomChar = (random.nextInt(26) + 97).toChar
              if (c.isUpper) randomChar.toUpper else randomChar
            } else {
              c
            }
          })
          prefix + str.substring(3)
        } else {
          val prefix = str.map(c => {
            if (String.valueOf(c).matches("[\u4e00-\u9fa5]")) {
              c
            } else if (c.isDigit) {
              random.nextInt(10).toString.charAt(0)
            } else if (c.isLetter) {
              val randomChar = (random.nextInt(26) + 97).toChar
              if (c.isUpper) randomChar.toUpper else randomChar
            } else {
              c
            }
          })
          prefix
        }
      }
      // 后四
      else if (start == 0 & end != 0) {
        if (str.length >= 4) {
          val suffix = str.substring(str.length - 4).map(c => {
            if (String.valueOf(c).matches("[\u4e00-\u9fa5]")) {
              c
            } else if (c.isDigit) {
              random.nextInt(10).toString.charAt(0)
            } else if (c.isLetter) {
              val randomChar = (random.nextInt(26) + 97).toChar
              if (c.isUpper) randomChar.toUpper else randomChar
            } else {
              c
            }
          })
          str.substring(0, str.length - 4) + suffix
        } else {
          val prefix = str.map(c => {
            if (String.valueOf(c).matches("[\u4e00-\u9fa5]")) {
              c
            } else if (c.isDigit) {
              random.nextInt(10).toString.charAt(0)
            } else if (c.isLetter) {
              val randomChar = (random.nextInt(26) + 97).toChar
              if (c.isUpper) randomChar.toUpper else randomChar
            } else {
              c
            }
          })
          prefix
        }
      }
      // 全部
      else {
        val prefix = str.map(c => {
          if (String.valueOf(c).matches("[\u4e00-\u9fa5]")) {
            c
          } else if (c.isDigit) {
            random.nextInt(10).toString.charAt(0)
          } else if (c.isLetter) {
            val randomChar = (random.nextInt(26) + 97).toChar
            if (c.isUpper) randomChar.toUpper else randomChar
          } else {
            c
          }
        })
        prefix
      }
    } else {
      //str
      null
    }
  }
}
