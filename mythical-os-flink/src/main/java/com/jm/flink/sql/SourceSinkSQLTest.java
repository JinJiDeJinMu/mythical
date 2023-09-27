package com.jm.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/25 13:53
 */
public class SourceSinkSQLTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);


        String sourceSql = "CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");";

        String sinkSql = "CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    sumVC INT \n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");";

        tableEnvironment.executeSql(sourceSql);
        tableEnvironment.executeSql(sinkSql);

        Table source = tableEnvironment.from("source");
        Table result = source
                .where($("id").isGreater(5))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sumVC"))
                .select($("id"), $("sumVC"));

        result.executeInsert("sink");
    }
}
