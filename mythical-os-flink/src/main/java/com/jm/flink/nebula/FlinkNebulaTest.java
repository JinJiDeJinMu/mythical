package com.jm.flink.nebula;

import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.nebula.catalog.NebulaCatalog;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.sink.NebulaVertexBatchOutputFormat;
import org.apache.flink.connector.nebula.source.NebulaEdgeSource;
import org.apache.flink.connector.nebula.source.NebulaSourceFunction;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaCatalogUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlinkNebulaTest {

    public static void main(String[] args) throws Exception {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setGraphAddress("192.168.201.12:9669")
                .setMetaAddress("192.168.201.12:9559")
                .build();
//        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions.NebulaClientOptionsBuilder()
//                .setMetaAddress("192.168.201.57:9559")
//                .build();
//        NebulaStorageConnectionProvider storageConnectionProvider = new NebulaStorageConnectionProvider(nebulaClientOptions);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        VertexExecutionOptions vertexExecutionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
//                .setGraphSpace("company_equity")
//                .setTag("company")
//                .setNoColumn(false)
//                .setFields(Arrays.asList())
//                .setLimit(100)
//                .build();
//
//        NebulaSourceFunction sourceFunction = new NebulaSourceFunction(storageConnectionProvider)
//                .setExecutionOptions(vertexExecutionOptions);
//
//        DataStreamSource<BaseTableRow> dataStreamSource = env.addSource(sourceFunction);
//
//        dataStreamSource.map(row->{
//            List<ValueWrapper> values = row.getValues();
//            Row record = new Row(2);
//            record.setField(0, values.get(0).asString());
//            record.setField(1, values.get(1).asString());
//            return record;
//        }).print();
//
//        try {
//            env.execute("NebulaStreamSource");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }

        write(nebulaClientOptions);
    }


    public static void write(NebulaClientOptions nebulaClientOptions) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        NebulaGraphConnectionProvider graphConnectionProvider = new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider = new NebulaMetaConnectionProvider(nebulaClientOptions);

        VertexExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("finance_services_test")
                .setTag("testTag")
                .setIdIndex(0)
                .setFields(Arrays.asList("id"))
                .setPositions(Arrays.asList(1))
                .setBatchSize(2)
                .build();


        NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);

        DataStream<List<String>> playerSource = constructVertexSourceData(env);
        DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new org.apache.flink.types.Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        env.execute("write nebula");
    }

    public static DataStream<List<String>> constructVertexSourceData(
            StreamExecutionEnvironment env) {
        List<List<String>> player = new ArrayList<>();
        List<String> fields1 = Arrays.asList("62", "aba");
        List<String> fields2 = Arrays.asList("63", "12");
        List<String> fields3 = Arrays.asList("64", "a321ba");
        List<String> fields4 = Arrays.asList("65", "43");
        List<String> fields5 = Arrays.asList("66", "54");
        player.add(fields1);
        player.add(fields2);
        player.add(fields3);
        player.add(fields4);
        player.add(fields5);
        DataStream<List<String>> playerSource = env.fromCollection(player);
        return playerSource;
    }

}
