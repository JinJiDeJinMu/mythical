package com.nebula.hs.udfs.encrypt

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

/**
 * 混洗脱敏:
 * ··对敏感数据（即敏感字段）的数据，进行跨行随机互换
 *
 * @author wzw
 * @date 2023/4/18
 */
object ShuffleData {
  var colType: String = _

  /**
   *
   * @param df   [[DataFrame]]
   * @param cols [[String]]
   * @return
   */
  def encrypt(df: DataFrame, cols: String): DataFrame = {
    df.schema.fields.foreach(x => {
      if (x.name == cols) {
        colType = x.dataType.typeName
      }
    })
    var oldDF = df.withColumn("rad", rand())
    oldDF = oldDF.withColumn("hs_id", row_number().over(Window.orderBy("rad")))

    val df1 = oldDF //df.withColumn("rad", rand())
      .orderBy("rad")

    val windowSpec: WindowSpec = Window.orderBy("win")
    val df2 = df1
      .withColumn("win", lit(1))
      .withColumn(cols, coalesce(col(cols), lit("hs_null")))
      .withColumn("next", lead(cols, 1).over(windowSpec))
      .withColumn(cols, coalesce(col("next"), first(cols).over(Window.orderBy("rad"))))
    val df3 = df2.drop("rad", "win", "next")
      .withColumn(cols, when(col(cols).equalTo("hs_null"), null).otherwise(col(cols)).cast(colType))
    val result = df3.as("new").join(
      //df.drop(cols)
      oldDF.drop(cols).as("old")
      , Seq("hs_id")
      , "inner"
    ).selectExpr(selectColumnTrans(df.columns, cols): _*)

    result.drop("hs_id")
  }

  /**
   * select字段重写
   *
   * @param cols       [[Array]].[[String]]
   * @param shuffleCol [[String]]
   * @return
   */
  def selectColumnTrans(cols: Array[String], shuffleCol: String): Array[String] = {
    cols.map(c => {
      if (c.equals(shuffleCol)) {
        s"new.${c}"
      } else {
        s"old.${c}"
      }
    })
  }
}
