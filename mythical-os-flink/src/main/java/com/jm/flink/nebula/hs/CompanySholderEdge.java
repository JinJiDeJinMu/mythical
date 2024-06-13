package com.jm.flink.nebula.hs;

import com.jm.flink.nebula.FlinkConnectorSinkNebula;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaEdgeBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class CompanySholderEdge {

    public static void main(String[] args) {

    }

    public static DataStream<Row> readCompanySholder(StreamExecutionEnvironment env){
        // 创建 JDBC 输入格式
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("org.postgresql.Driver")
                .setDBUrl("jdbc:postgresql://192.168.201.30:1921/dm")
                .setUsername("zhengyl")
                .setPassword("ZIf5yNb3hllGfdPL")
                .setFetchSize(2000)
                .setQuery("select company_code,sholder_code,sholder_name,proportion,con_amt,currency,paied_amt from dws_lget_company_sholder where is_delete=0 and sholder_type = '公司'")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                ))
                .finish();

        // 从 PostgreSQL 读取数据
        return env.createInput(jdbcInputFormat);
    }

    /**
     * sink Nebula Graph
     */
    public static void sinkEdgeData(StreamExecutionEnvironment env,
                                    DataStream<Row> dataStream) {
        NebulaClientOptions nebulaClientOptions = FlinkConnectorSinkNebula.getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("hsmap_atlas")
                .setEdge("company_sholder")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setRankIndex(4)
                .setFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7",
                        "col8", "col9", "col10", "col11", "col12", "col13", "col14"))
                .setPositions(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
                .setBatchSize(2000)
                .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("company_sholder_edge write nebula");
        } catch (Exception e) {
            System.exit(-1);
        }
    }
}
