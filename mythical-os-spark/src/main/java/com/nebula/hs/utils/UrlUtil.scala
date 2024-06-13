package com.nebula.hs.utils

object UrlUtil {
  def urlToMap(url: String): Map[String, String] = {
    if (url.contains("?")) {
      val params = url.split("\\?")(1)
      params.split("&").map { param =>
        val pair = param.split("=")
        pair(0) -> pair(1)
      }.toMap
    } else {
      Map.empty[String, String]
    }
  }
}
