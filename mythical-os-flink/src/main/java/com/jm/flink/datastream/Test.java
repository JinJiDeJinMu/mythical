package com.jm.flink.datastream;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;


import java.util.concurrent.TimeUnit;

/**
 * 一个dataStream同时写入到多个sink
 */
public class Test {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.fromElements("a", "b", "c", "d", "e");

        SinkFunction<String> consoleSink = new PrintSinkFunction<>();
        SinkFunction<String> fileSink = StreamingFileSink
                .forRowFormat(new Path("/Users/jinmu/Downloads/self/mythical/mythical-os-flink/src/main/java/com/jm/flink/datastream"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                .build();

        dataStream.addSink(consoleSink).name("console-sink");
        dataStream.addSink(fileSink).name("file-sink");

        env.execute("Multiple Sinks Example");
    }
}

