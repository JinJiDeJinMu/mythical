package com.jm.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/26 14:21
 */
public class UpsertKafkaTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);


        String test = "CREATE TABLE kafka_table (\n" +
                "  -- 元数据字段\n" +
                "  `topic` STRING METADATA VIRTUAL, -- 不指定 FROM\n" +
                "  `partition_id` STRING METADATA FROM 'partition' VIRTUAL, -- 指定 FROM\n" +
                "  `offset` BIGINT METADATA VIRTUAL,  -- 不指定 FROM\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL, -- 指定 FROM\n" +
                "  -- 业务字段\n" +
                "  `uid` STRING COMMENT '用户Id',\n" +
                "  `wid` STRING COMMENT '微博Id',\n" +
                "  `tm` STRING COMMENT '发微博时间',\n" +
                "  `content` STRING COMMENT '微博内容'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'jinmu_test',\n" +
                "  'properties.bootstrap.servers' = '192.168.110.41:9092,192.168.110.42:9092,192.168.110.43:9092',\n" +
                "  'properties.group.id' = 'kafka-meta-example',\n" +
                "  'value.format' = 'json',\n" +
                "  'scan.startup.mode' = 'earliest-offset', \n" +
                "  'value.json.ignore-parse-errors' = 'true'\n" +
                ");";

        String print = "CREATE TABLE print_table (\n" +
                "  `topic` STRING,\n" +
                "  `partition_id` STRING,\n" +
                "  `offset` BIGINT,\n" +
                "  `ts` TIMESTAMP(3),\n" +
                "  `uid` STRING COMMENT '用户Id',\n" +
                "  `wid` STRING COMMENT '微博Id',\n" +
                "  `tm` STRING COMMENT '发微博时间',\n" +
                "  `content` STRING COMMENT '微博内容'\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")";

        String upsertKafkaSource = "CREATE TABLE source (\n" +
                "    id     STRING,    \n" +
                "    name      STRING,          \n" +
                "    currenttime TIMESTAMP(3), \n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'jinmu_upsert_kafka',\n" +
                "  'properties.group.id' = 'group1',"+
                "  'properties.bootstrap.servers' = '192.168.110.42:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");";

        String upsertKafkaSink = "CREATE TABLE sink (\n" +
                "    id STRING, \n" +
                "    name STRING, \n" +
                "    currenttime TIMESTAMP(3) \n"+
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");";

        tableEnvironment.executeSql(test);
        tableEnvironment.executeSql(print);
        tableEnvironment.executeSql(upsertKafkaSource);
        tableEnvironment.executeSql(upsertKafkaSink);

        TableResult execute = tableEnvironment.from("kafka_table").insertInto("print_table").execute();

        System.out.println(execute.getJobClient().get().getJobStatus());
    }
}
