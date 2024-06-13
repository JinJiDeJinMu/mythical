package com.nebula.hs.common

/**
 * {{{
 *    加密算法类型
 * }}}
 *
 * @author wzw
 */
object EncryptType extends Enumeration {
  type CalculationModeEnum = Value
  val AES: Value = Value(0, "AES")
  val SM4: Value = Value(1, "SM4")
  val MD5: Value = Value(2, "MD5")
  val MaskWithChar: Value = Value(3, "MaskWithChar")
  val MaskWithRadChar: Value = Value(4, "MaskWithRadChar")
  val NumericalTrans: Value = Value(5, "NumericalTrans")
  val NullInOrDel: Value = Value(6, "NullInOrDel")
  val Shuffle: Value = Value(7, "Shuffle")
}

object EncryptTypeConstant {
  val AES: String = EncryptType.AES.toString
  val SM4: String = EncryptType.SM4.toString
  val MD5: String = EncryptType.MD5.toString
  val MaskWithChar: String = EncryptType.MaskWithChar.toString
  val MaskWithRadChar: String = EncryptType.MaskWithRadChar.toString
  val NumericalTrans: String = EncryptType.NumericalTrans.toString
  val NullInOrDel: String = EncryptType.NullInOrDel.toString
  val Shuffle: String = EncryptType.Shuffle.toString
}

/**
 * 调用方式示例
 */
object EnumTest {
  def main(args: Array[String]): Unit = {
    println("枚举使用方式：" + EncryptType.AES.toString)
    println("常量控制使用方式：" + EncryptTypeConstant.AES)

    val encryptType = "AES"
    encryptType match {
      case EncryptTypeConstant.AES => println("1")
      case EncryptTypeConstant.SM4 => println("2")
      case EncryptTypeConstant.MD5 => println("3")
      case EncryptTypeConstant.NullInOrDel => println("4")
      case EncryptTypeConstant.MaskWithChar => println("5")
      case EncryptTypeConstant.MaskWithRadChar => println("6")
      case EncryptTypeConstant.NumericalTrans => println("7")
      case EncryptTypeConstant.Shuffle => println("8")
    }
  }
}