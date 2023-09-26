package com.jm.flink.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/25 20:14
 */
public class MyFlinkSourceFunction extends RichSourceFunction {

    @Override
    public void run(SourceContext ctx) throws Exception {
        //开始运行
    }

    @Override
    public void cancel() {
        //取消任务
    }
}
