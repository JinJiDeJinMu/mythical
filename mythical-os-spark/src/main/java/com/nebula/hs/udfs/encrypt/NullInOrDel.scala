package com.nebula.hs.udfs.encrypt

/**
 * 对敏感数据进行空值插入或删除(置空)操作
 * ··deletionFlag = false -> null值插入
 * ··deletionFlag = true -> 置空
 *
 * @author wzw
 * @date 2023/4/17
 */
object NullInOrDel {
  def encrypt(columnName: String
              , deletionFlag: Boolean): String = {
    //    if (columnName != "null"){
    if (deletionFlag) {
      // 如果是删除操作，直接返回空字符串
      ""
    } else {
      // 否则进行空值插入操作，将该敏感字段的数据置为null
      null
    }
    //    }else {
    //      null
    //    }
  }
}
