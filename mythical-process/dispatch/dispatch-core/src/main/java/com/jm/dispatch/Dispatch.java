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
     * 运行
     */
    void run();


    /**
     * 取消
     */
    void cancel();
}
