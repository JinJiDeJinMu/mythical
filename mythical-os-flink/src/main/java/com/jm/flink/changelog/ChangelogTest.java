package com.jm.flink.changelog;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * TODO
 * 构建changelog流(insert、update、delete) 写入到upsert-kafka
 * @Author jinmu
 * @Date 2023/9/27 13:36
 */
public class ChangelogTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<Row> streamSource = env.fromElements(
                Row.ofKind(RowKind.INSERT, "111", "zhs"),
                Row.ofKind(RowKind.INSERT, "222", "lisi"),
                Row.ofKind(RowKind.UPDATE_BEFORE, "111", "zhs"),
                Row.ofKind(RowKind.UPDATE_AFTER, "111", "zhs-new"),
                Row.ofKind(RowKind.INSERT, "333", "ww"),
                Row.ofKind(RowKind.DELETE, "222","lisi"));

        Table table = tableEnvironment.fromChangelogStream(streamSource, Schema.newBuilder().primaryKey("f0").build(), ChangelogMode.all());

        tableEnvironment.createTemporaryView("test",table);


        String upsertKafkaSource = "CREATE TABLE sink (\n" +
                "    id     STRING,    \n" +
                "    name      STRING,          \n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'jinmu_upsert_kafka',\n" +
                "  'properties.group.id' = 'group1',"+
                "  'properties.bootstrap.servers' = '192.168.110.42:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");";

        tableEnvironment.executeSql(upsertKafkaSource);

        tableEnvironment.sqlQuery("select f0 as id,f1 as name from test").executeInsert("sink");
    }
}
