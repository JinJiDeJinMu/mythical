package com.jm.flink.datastream;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcRowOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class JdbcOperator {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JdbcInputFormat inputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306")
                .setUsername("root")
                .setPassword("root")
                .setQuery("select id,dq from flink.ajxx_xs ")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                .finish();
        //读取jdbc
        DataStream<Row> dataSource = env.createInput(inputFormat);



        JdbcRowOutputFormat jdbcRowOutputFormat = JdbcRowOutputFormat.buildJdbcOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306")
                .setUsername("root")
                .setPassword("root")
                .setQuery("select id,dq from flink.ajxx_xs ")
                .finish();

    }
}
