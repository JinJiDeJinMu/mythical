package com.nebula.hs.etl

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
trait EtlPlugin[T] extends Serializable {
  protected var config: T = _

  def getConfig: T = config

  def setConfig(config: T): Unit = {
    this.config = config
  }

}
