package com.jm.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
 * @Date 2023/9/26 10:50
 */
public class KeyedListStateTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = environment.socketTextStream("localhost", 9999);

        socketTextStream.flatMap(
                        (FlatMapFunction<String, KeyedValueStateTest.WordWithCount>)
                                (value, out) -> {
                                    for (String word : value.split("\\s")) {
                                        out.collect(new KeyedValueStateTest.WordWithCount(word, 1L));
                                    }
                                },
                        Types.POJO(KeyedValueStateTest.WordWithCount.class))
                .keyBy(value -> value.word)
                .process(new KeyedProcessFunction<String, KeyedValueStateTest.WordWithCount, KeyedValueStateTest.WordWithCount>() {
                   ListState<Integer> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("test-keyed-list",Types.INT));
                    }

                    @Override
                    public void processElement(KeyedValueStateTest.WordWithCount value, KeyedProcessFunction<String, KeyedValueStateTest.WordWithCount, KeyedValueStateTest.WordWithCount>.Context ctx, Collector<KeyedValueStateTest.WordWithCount> out) throws Exception {
                        lastValueState.add(value.count.intValue());

                        Iterable<Integer> iterable = lastValueState.get();
                        System.out.println("开始 ,key =" + value.word);
                        iterable.forEach(e->{
                            System.out.println(e);
                        });

                        System.out.println("结束 ,key =" + value.word);
                        //输出
                        out.collect(value);
                    }
                }).print();

        environment.execute();

    }


    public static class WordWithCount {

        public String word;
        public long count;

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
