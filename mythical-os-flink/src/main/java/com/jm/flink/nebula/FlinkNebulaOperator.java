package com.jm.flink.nebula;

import com.vesoft.nebula.client.storage.data.BaseTableRow;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.source.NebulaInputRowFormat;
import org.apache.flink.connector.nebula.source.NebulaInputTableRowFormat;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class FlinkNebulaOperator {
    private static NebulaStorageConnectionProvider storageConnectionProvider;
    private static NebulaStorageConnectionProvider storageConnectionProviderCaSSL;
    private static NebulaStorageConnectionProvider storageConnectionProviderSelfSSL;
    private static ExecutionOptions vertexExecutionOptions;
    private static ExecutionOptions edgeExecutionOptions;


    public static void nebulaVertexBatchSource() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // get Nebula vertex data in flink Row format
        NebulaInputRowFormat inputRowFormat = new NebulaInputRowFormat(storageConnectionProvider,
                vertexExecutionOptions);
        DataSource<Row> rowDataSource = env.createInput(inputRowFormat);
        rowDataSource.print();
        System.out.println("rowDataSource count: " + rowDataSource.count());

        // get Nebula vertex data in nebula TableRow format
        NebulaInputTableRowFormat inputFormat =
                new NebulaInputTableRowFormat(storageConnectionProvider,
                        vertexExecutionOptions);
        DataSource<BaseTableRow> dataSource = env.createInput(inputFormat);
        dataSource.print();
        System.out.println("datasource count: " + dataSource.count());
    }

    /**
     * read Nebula Graph edge as batch data source
     */
    public static void nebulaEdgeBatchSource() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // get Nebula edge data in flink Row format
        NebulaInputRowFormat inputFormat = new NebulaInputRowFormat(storageConnectionProvider,
                edgeExecutionOptions);
        DataSource<Row> dataSourceRow = env.createInput(inputFormat);
        dataSourceRow.print();
        System.out.println("datasource count: " + dataSourceRow.count());

        // get Nebula edge data in Nebula TableRow format
        NebulaInputTableRowFormat inputTableFormat =
                new NebulaInputTableRowFormat(storageConnectionProvider,
                        edgeExecutionOptions);
        DataSource<BaseTableRow> dataSourceTableRow = env.createInput(inputTableFormat);
        dataSourceTableRow.print();
        System.out.println("datasource count: " + dataSourceTableRow.count());
    }

    public static void initConfig() {
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setMetaAddress("192.168.201.57:9559")
                        .build();
        storageConnectionProvider =
                new NebulaStorageConnectionProvider(nebulaClientOptions);

        NebulaClientOptions nebulaClientOptionsWithCaSSL =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setMetaAddress("192.168.201.57:9559")
                        .build();
        storageConnectionProviderCaSSL =
                new NebulaStorageConnectionProvider(nebulaClientOptionsWithCaSSL);

        NebulaClientOptions nebulaClientOptionsWithSelfSSL =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setMetaAddress("192.168.201.57:9559")
//                        .setEnableMetaSSL(true)
//                        .setEnableStorageSSL(true)
//                        .setSSLSignType(SSLSignType.SELF)
//                        .setSelfSignParam("example/src/main/resources/ssl/selfsigned.pem",
//                                "example/src/main/resources/ssl/selfsigned.key",
//                                "vesoft")
                        .build();
        storageConnectionProviderSelfSSL =
                new NebulaStorageConnectionProvider(nebulaClientOptionsWithSelfSSL);

        // read no property
        vertexExecutionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("hs_prod")
                .setTag("delta")
                .setNoColumn(false)
                .setFields(Arrays.asList())
                .setLimit(100)
                .build();

        // read specific properties
        // if you want to read all properties, config: setFields(Arrays.asList())
        edgeExecutionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("hs_prod")
                .setEdge("table_flow")
                //.setFields(Arrays.asList("col1", "col2","col3"))
                .setFields(Arrays.asList())
                .setLimit(100)
                .build();

    }
}
