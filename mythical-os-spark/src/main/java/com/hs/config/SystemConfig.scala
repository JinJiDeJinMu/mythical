/*
 *@Author   : DoubleTrey
 *@Time     : 2023/2/14 16:46
 */

package com.hs.config

/**
 * 系统配置项
 * 罗列可调整的系统配置信息，各项值常做为默认值
 */
object SystemConfig {
  val warehouseRootPath = "/user/hive/warehouse"
  val tablePathFormat: String = warehouseRootPath + "/%s.db/%s"

  val offsetRootPath = "/delta/_offset"
  val offsetPathFormat: String = offsetRootPath + "/%s/%s"

  val defaultEncoding = "utf-8"
}
