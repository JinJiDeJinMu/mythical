package com.jm.demo.pipeline;

import java.io.IOException;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/3 11:17
 */
public interface Work<T, M> {


    M doWork(T t) throws IOException;
}
