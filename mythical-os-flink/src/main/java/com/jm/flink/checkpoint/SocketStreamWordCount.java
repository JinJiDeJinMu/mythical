package com.jm.flink.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * TODO
 * doc
 *  https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/state/checkpoints/
 * @Author jinmu
 * @Date 2023/9/22 10:02
 */
public class SocketStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //自带UI界面
        //StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        /**
         * 开启checkpoint
         */
        // 1、启用检查点: 默认是barrier对齐的，周期为5s, 精准一次
        environment.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        // 2、设置checkpoint路径
        checkpointConfig.setCheckpointStorage("file:///Users/jinmu/Downloads/test-flink-checkpoint");
        // 3、checkpoint的超时时间: 默认10分钟
        checkpointConfig.setCheckpointTimeout(60000);
        // 4、同时运行中的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 5、最小等待间隔: 上一轮checkpoint结束 到 下一轮checkpoint开始 之间的间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 6、取消作业时，checkpoint的数据 是否保留在外部系统
        // DELETE_ON_CANCELLATION:主动cancel时，删除存在外部系统的chk-xx目录 （如果是程序突然挂掉，不会删）
        // RETAIN_ON_CANCELLATION:主动cancel时，外部系统的chk-xx目录会保存下来
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 7、允许 checkpoint 连续失败的次数，默认0--》表示checkpoint一失败，job就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        // TODO 开启 非对齐检查点（barrier非对齐）
        // 开启的要求： Checkpoint模式必须是精准一次，最大并发必须设为1
        //checkpointConfig.enableUnalignedCheckpoints();
        // 开启非对齐检查点才生效： 默认0，表示一开始就直接用 非对齐的检查点
        // 如果大于0， 一开始用 对齐的检查点（barrier对齐）， 对齐的时间超过这个参数，自动切换成 非对齐检查点（barrier非对齐）
        //checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));

        /**
         * flink 提供两种stateBackend
         * HashMapStateBackend 默认内存hash
         * EmbeddedRocksDBStateBackend 生产环境推荐rocksdb,目前唯一提供增量检查点的后端
         *
         * flink-config.yaml中对应如下
         *    state.backend.type: hashmap
         *    state.checkpoints.dir: hdfs://namenode-host:port/flink-checkpoints  这里和上面checkpoint目录一直
         *    state.backend.incremental: true   默认是false，用于指定是否开启增量checkpoint
         *    state.backend.local-recovery: false  是否启用本地恢复。默认为false。启用该配置可以加快作业的故障恢复速度
         *
         */

        //是否开启增量checkpoint
        boolean enableIncrementalCheckpointing = true;
        //设置rocksDB属性  EmbeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)
        environment.setStateBackend(new EmbeddedRocksDBStateBackend(enableIncrementalCheckpointing));


        /**
         * 开启 changelog
         *
         *    state.backend.changelog.enabled: true
         *    state.backend.changelog.storage: filesystem # currently, only filesystem and memory (for tests) are supported
         *    dstl.dfs.base-path: hdfs://hadoop102:8020/changelog
         *    execution.checkpointing.max-concurrent-checkpoints: 1
         *    execution.savepoint-restore-mode: CLAIM   不支持 NO_CLAIM 模式
         */
        //environment.enableChangelogStateBackend(true);



        DataStreamSource<String> socketTextStream = environment.socketTextStream("localhost", 9999);
//
//        SingleOutputStreamOperator<WordWithCount> streamOperator = socketTextStream.flatMap(
//                        (FlatMapFunction<String, WordWithCount>)
//                                (value, out) -> {
//                                    for (String word : value.split("\\s")) {
//                                        out.collect(new WordWithCount(word, 1L));
//                                    }
//                                },
//                        Types.POJO(WordWithCount.class))
//                .keyBy(value -> value.word)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .reduce((a, b) -> new WordWithCount(a.word, a.count + b.count))
//                .returns(WordWithCount.class);
//
//        streamOperator.uid("test-uid").print().setParallelism(1);

        socketTextStream.flatMap(
                (String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }
        )
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        environment.execute("socket wordCount");
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
