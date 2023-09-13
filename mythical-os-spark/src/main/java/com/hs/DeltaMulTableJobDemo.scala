package com.hs

import com.hs.config.DeltaMulTableConfig
import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object DeltaMulTableJobDemo {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("任务配置信息异常！")
    }

    val params = args(0)
    println("任务配置信息：" + params)
    //val params ="{\"appName\": \"deltaJobTest\",\"tablePathFormat\": \"/user/hive/warehouse/%s.db/%s\",\"sourceDbTableMap\":\"tcst_dwd_database:dwd_tcst_close_standard_info_all,tcst_dwd_database:dwd_tcst_tech_standard_info_all\",\"sinkDbTableMap\":\"test:test\",\"writeMode\":\"append\",\"mergeKeys\":\"standard_no\",\"sortColumns\":\"standard_no:-1\"}"

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobConf = JsonMethods.parse(params).extract[DeltaMulTableConfig]
    println(jobConf)

    // TODO : JOB CONFIG
    val job = DeltaMulTableJob(jobConf)

    // TODO : COMPUTER AND JOIN
    val resultDf: DataFrame = job.calculation(
      s"""
         |select  t0.bu, t0.creator, t0.operator, t0.id, t0.is_delete, t0.create_time
         |    , t0.update_time, t0.standard_no, t0.title_cn, t0.title_en,t0.publish_date
         |    , t0.public_section, t0.enforce_date, t0.first_publish_date, t0.standard_state
         |    , t0.standard_state_code, t0.plan_no, t0.new_standard_no, t0.orig_standard_no
         |    , t0.end_date as thru_date, t0.adopted_intl_standard_no, t0.adopted_intl_standard_name
         |    , t0.adopted_degree, t0.adopted_intl_standard_type, t0.ics_code, t0.ccs_code
         |    , t0.standard_classify as standard_class, t0.admin_org as governor_type, t0.belong_org
         |    , t0.draft_org, t0.draft_person, t0.standard_nature, t0.standard_type, t0.standard_type_code
         |    , t0.industry_type, t0.industry_type_code, t0.appl_scope, t0.reg_no, t0.reg_notice, t0.region
         |    , t0.province, t0.city, t0.province_code, t0.city_code, t0.execute_org, t0.propose_org, t0.intl_org
         |    , t0.version, t0.publish_org, t0.standard_lng, t0.source_url, t0.pdf_source_url, t0.source_site, t0.revising
         |    , t0.standard_oneid, t1.close_section
         |from (
         |   select lt.*,datediff(current_date(), lt.publish_date) as public_section
         |   from dwd_tcst_tech_standard_info_all lt
         |   where lt.is_delete='0'
         |) t0
         |join (
         |    select standard_code as standard_no
         |        ,count(close_standard_code) as close_section
         |    from dwd_tcst_close_standard_info_all rt
         |    where rt.is_delete='0'
         |    group by standard_code
         |) t1
         |on t0.standard_no = t1.standard_no
         |""".stripMargin)
    resultDf.show(false)

    // TODO : WRITE TO DELTA TABLE
    job.saveToDelta(resultDf)
  }
}
