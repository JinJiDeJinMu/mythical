package com.jm.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/26 09:36
 */
public class KeyedValueStateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = environment.socketTextStream("localhost", 9999);

          socketTextStream.flatMap(
                        (FlatMapFunction<String, WordWithCount>)
                                (value, out) -> {
                                    for (String word : value.split("\\s")) {
                                        out.collect(new WordWithCount(word, 1L));
                                    }
                                },
                        Types.POJO(WordWithCount.class))
                .keyBy(value -> value.word)
                        .process(new KeyedProcessFunction<String, WordWithCount, WordWithCount>() {
                            ValueState<Integer> lastValueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("test-keyed-value", Types.INT));
                            }

                            @Override
                            public void processElement(WordWithCount value, KeyedProcessFunction<String, WordWithCount, WordWithCount>.Context ctx, Collector<WordWithCount> out) throws Exception {

                                int lastValue = lastValueState.value() == null? 1: lastValueState.value();
                                System.out.println("key = " + value.word + ",,求和lastValue = " + lastValue);

                                lastValueState.update(lastValue + 1);
                                //输出
                                out.collect(value);
                            }
                        }).print();

          environment.execute();

    }

    public static class WordWithCount {

        public String word;
        public Long count;

        @SuppressWarnings("unused")
        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }

}
