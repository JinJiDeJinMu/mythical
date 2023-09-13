package com.hs.udfs.encrypt

/**
 * 指定字符掩码
 *
 * @author wzw
 * @date 2023/4/17
 */
object MaskWithChar {
  def encrypt(s: String
              , maskType: String = "*"
              , start: Int = 0
              , end: Int = 0): String = {
    if (s != null) {
      val len = s.length()
      val min: Int = start.min(end)
      val max: Int = start.max(end)
      val sum = start + end
      if (len >= sum) {
        val prefix = s.substring(0, start)
        val suffix = s.substring(len - end, len)
        val mask = maskType * (len - start - end)
        prefix + mask + suffix
      } else if (len <= min) {
        maskType * len
      } else if (len > min && len <= max) {
        maskType * len
      } else {
        if (start < end) {
          s.substring(0, start) + maskType * (len - start)
        } else {
          maskType * (len - end) + s.substring(len - end, len)
        }
      }
    } else {
      //s
      null
    }
  }
}
