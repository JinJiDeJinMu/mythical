package com.jm.flink.process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/22 15:45
 */
public class TestProcessApi {

    public static void main(String[] args) throws Exception {
        String path = "file:///Users/jinmu/Downloads/test-flink-checkpoint/fe252d9b7de94a56989154d27520c613/chk-18";
        boolean enableIncrementalCheckpointing = true;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        try {
            SavepointReader savepoint = SavepointReader.read(env,path , new EmbeddedRocksDBStateBackend(enableIncrementalCheckpointing));

            DataStream<Object> dataStream = savepoint.readKeyedState(OperatorIdentifier.forUid("test-uid"),
                    new KeyedStateReaderFunction<Object, Object>() {
                        @Override
                        public void open(Configuration configuration) throws Exception {
                            System.out.println("开始读取");
                        }

                        @Override
                        public void readKey(Object o, Context context, Collector<Object> collector) throws Exception {
                            System.out.println("读取key," + o);
                        }
                    });

            dataStream.print().setParallelism(1);

            env.execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
