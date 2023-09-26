package com.jm.flink.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * TODO
 *
 *  还有很多sink 类似TwoPhaseCommitSinkFunction
 * @Author jinmu
 * @Date 2023/9/25 20:16
 */

public class MyFlinkSinkFunction extends RichSinkFunction {


    @Override
    public void invoke(Object value, Context context) throws Exception {
        super.invoke(value, context);
    }
}
