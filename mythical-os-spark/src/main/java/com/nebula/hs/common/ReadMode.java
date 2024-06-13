package com.nebula.hs.common;

/**
 * 数据源读取方式
 */
public enum ReadMode {
    /**
     * 增量读取，基于可做增量依据的数据源字段（时间、有序id等）
     */
    incr,
    /**
     * 全量读取
     */
    full
}
