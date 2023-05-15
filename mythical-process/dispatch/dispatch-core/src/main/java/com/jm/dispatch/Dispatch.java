package com.jm.dispatch;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 20:31
 */
public interface Dispatch {

    /**
     *初始化
     */
    void init();

    /**
     * 运行前置
     */
    void preRun();

    /**
     * 运行
     */
    void run();

    /**
     * 运行后置
     */
    void postRun();

    /**
     * 取消
     */
    void cancel();
}
