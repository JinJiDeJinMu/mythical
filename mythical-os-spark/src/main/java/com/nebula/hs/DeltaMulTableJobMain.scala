package com.nebula.hs

import com.hs.config.DeltaMulTableConfig
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

/**
 * description:{{{
 *   DeltaJob 统一入口,按照参传示例入对应任务的参数即可
 *      sql可以作为参数传入或者本地化编写
 * }}}
 * 参数示例:{{{
 *    {
 *       "appName": "ods_fina_merge_event_all",
 *       "tablePathFormat": "/user/hive/warehouse/%s.db/%s",
 *       "sourceDbTableMap": "fina_stg:stg_fina_merge_event_di,dim_stg:dim_stds_exchange_rate",
 *       "sinkDbTableMap": "ods:ods_fina_merge_event_all",
 *       "sql": "with t0 as (select id,trade_currency,trade_value from stg_fina_merge_event_di t where is_delete='0') ,t1 as (select currency, trade_date, rmb_central_parity from (SELECT currency, trade_date, rmb_central_parity, is_delete, row_number() over(PARTITION BY currency ORDER BY trade_date desc) rn from dim_stds_exchange_rate WHERE is_delete = '0') t WHERE t.rn = 1) select t0.id,CASE t0.trade_currency WHEN 'CNY' THEN round(t0.trade_value * 1000000,4) ELSE  round(t0.trade_value * 1000000 * t1.rmb_central_parity,4) END AS trade_value_cal from  t0 left join t1 on t0.trade_currency = t1.currency",
 *       "writeMode": "append",
 *       "mergeKeys": "id",
 *       "sortColumns": "update_time:-1"
 *    }
 *    备注:注意有无sql参数的区别
 * }}}
 *
 * @author wzw
 */
object DeltaMulTableJobMain {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)

    // 1. job参数解析
    //val params = "{\"appName\": \"ods_fina_merge_event_all\",\"tablePathFormat\": \"/user/hive/warehouse/%s.db/%s\",\"sourceDbTableMap\": \"fina_stg:stg_fina_merge_event_di,dim_stg:dim_stds_exchange_rate\",\"sinkDbTableMap\": \"ods:ods_fina_merge_event_all\",\"sql\":\"with t0 as (select id,trade_currency,trade_value from stg_fina_merge_event_di t where is_delete='0') ,t1 as (select currency, trade_date, rmb_central_parity from (SELECT currency, trade_date, rmb_central_parity, is_delete, row_number() over(PARTITION BY currency ORDER BY trade_date desc) rn from dim_stds_exchange_rate WHERE is_delete = '0') t WHERE t.rn = 1) select t0.id,CASE t0.trade_currency WHEN 'CNY' THEN round(t0.trade_value * 1000000,4) ELSE  round(t0.trade_value * 1000000 * t1.rmb_central_parity,4) END AS trade_value_cal from  t0 left join t1 on t0.trade_currency = t1.currency\",\"writeMode\": \"append\",\"mergeKeys\": \"id\",\"sortColumns\": \"update_time:-1\"}"
    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[DeltaMulTableConfig]
    println(jobConf)

    // 2. JOB CONFIG
    val job = DeltaMulTableJob(jobConf)

    // 3. COMPUTER AND JOIN: 支持参数传入sql的方式/非参数传入sql的方式
    val sql = if (jobConf.sql != "") {
      """
        |
        |
        |
        |""".stripMargin
    } else {
      jobConf.sql
    }
    val resultDf = job.calculation(sql)

    // 4. WRITE TO DELTA TABLE
    job.saveToDelta(resultDf)
  }
}
