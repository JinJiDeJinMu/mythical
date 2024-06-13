/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.jm.flink.nebula;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaEdgeBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.sink.NebulaVertexBatchOutputFormat;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * make sure your nebula graph has create Space, space schema:
 *
 * <p>"CREATE SPACE `flinkSink` (partition_num = 100, replica_factor = 3, charset = utf8,
 * collate = utf8_bin, vid_type = INT64, atomic_edge = false)"
 *
 * <p>"USE `flinkSink`"
 *
 * <p>"CREATE TAG IF NOT EXISTS person(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 *
 * <p>"CREATE EDGE IF NOT EXISTS friend(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 */
public class FlinkConnectorSinkNebula {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorSinkNebula.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        DataStream<List<String>> playerSource = testData(env);

        //DataStream<Row> dataStream = readPGData(env);

        sinkEdgeDataTest(env,playerSource);
        //sinkVertexData(env, playerSource);
//        updateVertexData(env, playerSource);
//        deleteVertexData(env, playerSource);
//
//        DataStream<List<String>> friendSource = constructEdgeSourceData(env);
//        sinkEdgeData(env, friendSource);
//        updateEdgeData(env, friendSource);
//        deleteEdgeData(env, friendSource);
    }



    public static DataStream<Row> readPGData(StreamExecutionEnvironment env){
        // 创建 JDBC 输入格式
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("org.postgresql.Driver")
                .setDBUrl("jdbc:postgresql://192.168.201.30:1921/dm")
                .setUsername("zhengyl")
                .setPassword("ZIf5yNb3hllGfdPL")
                .setFetchSize(2000)
                .setQuery("SELECT company_code, company_name, company_sname FROM dws.dws_lget_company_info limit 10")
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
     * construct flink data source
     */
    public static DataStream<List<String>> constructVertexSourceData(
            StreamExecutionEnvironment env) {
        List<List<String>> player = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields2 = Arrays.asList("62", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields3 = Arrays.asList("63", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields4 = Arrays.asList("64", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields5 = Arrays.asList("65", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields6 = Arrays.asList("66", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields7 = Arrays.asList("67", "李四", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0",
                "11:12:12", "polygon((0 1,1 2,2 3,0 1))");
        List<String> fields8 = Arrays.asList("68", "aba", "张三", "1", "1111", "22222", "6412233",
                "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0", "11:12:12",
                "POLYGON((0 1,1 2,2 3,0 1))");
        player.add(fields1);
        player.add(fields2);
        player.add(fields3);
        player.add(fields4);
        player.add(fields5);
        player.add(fields6);
        player.add(fields7);
        player.add(fields8);
        DataStream<List<String>> playerSource = env.fromCollection(player);
        return playerSource;
    }

    public static NebulaClientOptions getClientOptions() {
        // not enable ssl for graph
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("192.168.201.12:9669")
                        .setMetaAddress("192.168.201.12:9559")
                        .build();
        return nebulaClientOptions;
    }

    /**
     * sink Nebula Graph with default INSERT mode
     */
    public static void sinkVertexDataTest(StreamExecutionEnvironment env,
                                      DataStream<Row> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);
        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("test_company")
                        .setTag("company")
                        .setIdIndex(0)
                        .setFields(Arrays.asList("id", "name", "code"))
                        .setPositions(Arrays.asList(0, 1, 2))
                        .setBatchSize(2000)
                        .build();


        NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);

        playerSource.addSink(nebulaSinkFunction);
        try {
            env.execute("Write Nebula");
        } catch (Exception e) {
            LOG.error("error when write Nebula Graph, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph
     */
    public static void sinkEdgeDataTest(StreamExecutionEnvironment env,
                                    DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test_company")
                .setEdge("test_edge")
                .setSrcIndex(1)
                .setDstIndex(0)
                .setRankIndex(0)
                .setFields(Arrays.asList("id","name"))
                .setPositions(Arrays.asList(0, 2))
                .setBatchSize(2000)
                .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Write Nebula");
        } catch (Exception e) {
            LOG.error("error when write Nebula Graph, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph with default INSERT mode
     */
    public static void sinkVertexData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

//        VertexExecutionOptions executionOptions =
//                new VertexExecutionOptions.ExecutionOptionBuilder()
//                        .setGraphSpace("hsmap_atlas")
//                        .setTag("company")
//                        .setIdIndex(0)
//                        .setFields(Arrays.asList("company_code", "company_name", "company_sname", "uni_code", "tax_code", "found_date",
//                                "reg_address", "office_address", "legal_rep", "phone", "email"))
//                        .setPositions(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
//                        .setBatchSize(2000)
//                        .build();


        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("test_company")
                        .setTag("company")
                        .setIdIndex(0)
                        .setFields(Arrays.asList("id", "name", "code"))
                        .setPositions(Arrays.asList(0, 1, 2))
                        .setBatchSize(2000)
                        .build();


        NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Write Nebula");
        } catch (Exception e) {
            LOG.error("error when write Nebula Graph, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph with UPDATE mode
     */
    public static void updateVertexData(StreamExecutionEnvironment env,
                                        DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("flinkSink")
                        .setTag("person")
                        .setIdIndex(0)
                        .setFields(Arrays.asList("col1", "col2"))
                        .setPositions(Arrays.asList(1, 2))
                        .setWriteMode(WriteModeEnum.UPDATE)
                        .setBatchSize(2)
                        .build();

        NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Update Nebula Vertex");
        } catch (Exception e) {
            LOG.error("error when update Nebula Graph Vertex, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph with DELETE mode
     */
    public static void deleteVertexData(StreamExecutionEnvironment env,
                                        DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("flinkSink")
                        .setTag("person")
                        .setIdIndex(0)
                        .setFields(Arrays.asList("col1", "col2"))
                        .setPositions(Arrays.asList(1, 2))
                        .setWriteMode(WriteModeEnum.DELETE)
                        .setBatchSize(2)
                        .build();

        NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Update Nebula Vertex");
        } catch (Exception e) {
            LOG.error("error when update Nebula Graph Vertex, ", e);
            System.exit(-1);
        }
    }


    /**
     * construct flink data source
     */
    public static DataStream<List<String>> testData(StreamExecutionEnvironment env) {
        List<List<String>> friend = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "111", "aaaa");
        List<String> fields2 = Arrays.asList("62", "222", "bbbb");
        List<String> fields3 = Arrays.asList("63", "333", "ccc");
        List<String> fields4 = Arrays.asList("64", "444", "ddd");
        friend.add(fields1);
        friend.add(fields2);
        friend.add(fields3);
        friend.add(fields4);
        DataStream<List<String>> playerSource = env.fromCollection(friend);
        return playerSource;
    }

    /**
     * construct flink data source
     */
    public static DataStream<List<String>> constructEdgeSourceData(StreamExecutionEnvironment env) {
        List<List<String>> friend = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "62", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields2 = Arrays.asList("62", "63", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields3 = Arrays.asList("63", "64", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields4 = Arrays.asList("64", "65", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields5 = Arrays.asList("65", "66", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields6 = Arrays.asList("66", "67", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields7 = Arrays.asList("67", "68", "李四", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0",
                "11:12:12", "polygon((0 1,1 2,2 3,0 1))");
        List<String> fields8 = Arrays.asList("68", "61", "aba", "张三", "1", "1111", "22222",
                "6412233",
                "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0", "11:12:12",
                "POLYGON((0 1,1 2,2 3,0 1))");
        friend.add(fields1);
        friend.add(fields2);
        friend.add(fields3);
        friend.add(fields4);
        friend.add(fields5);
        friend.add(fields6);
        friend.add(fields7);
        friend.add(fields8);
        DataStream<List<String>> playerSource = env.fromCollection(friend);
        return playerSource;
    }

    /**
     * sink Nebula Graph
     */
    public static void sinkEdgeData(StreamExecutionEnvironment env,
                                    DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setRankIndex(4)
                .setFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7",
                        "col8", "col9", "col10", "col11", "col12", "col13", "col14"))
                .setPositions(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
                .setBatchSize(2)
                .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Write Nebula");
        } catch (Exception e) {
            LOG.error("error when write Nebula Graph, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph with UPDATE mode
     */
    public static void updateEdgeData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setRankIndex(4)
                .setFields(Arrays.asList("col1", "col2"))
                .setPositions(Arrays.asList(2, 3))
                .setWriteMode(WriteModeEnum.UPDATE)
                .setBatchSize(2)
                .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Update Nebula Vertex");
        } catch (Exception e) {
            LOG.error("error when update Nebula Graph Vertex, ", e);
            System.exit(-1);
        }
    }


    /**
     * sink Nebula Graph with DELETE mode
     */
    public static void deleteEdgeData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setRankIndex(4)
                .setFields(Arrays.asList("col1", "col2"))
                .setPositions(Arrays.asList(2, 3))
                .setWriteMode(WriteModeEnum.DELETE)
                .setBatchSize(2)
                .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Update Nebula Vertex");
        } catch (Exception e) {
            LOG.error("error when update Nebula Graph Vertex, ", e);
            System.exit(-1);
        }
    }
}
