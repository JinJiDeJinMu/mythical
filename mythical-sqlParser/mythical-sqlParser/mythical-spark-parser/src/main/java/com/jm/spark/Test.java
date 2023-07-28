package com.jm.spark;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/4 10:45
 */
public class Test {

    public static void main(String[] args) {
        //String sql = "SELECT id,name FROM b";
        String sql = "select a.c1 AS cc,a.c2 ,b.c3 FROM a LEFT JOIN (SELECT c1,c3,c4,c5,id FROM d) as b on a.id = b.id";


        //String sql = "create table test.sale_detail_like like demo.sale_detail";

        SparkSqlHelper sparkSqlHelper = new SparkSqlHelper();

        String data = sparkSqlHelper.getStatementData(sql);
        System.out.println(data);

    }
}
