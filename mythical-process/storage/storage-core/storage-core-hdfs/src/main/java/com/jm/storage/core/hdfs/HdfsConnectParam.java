package com.jm.storage.core.hdfs;

import lombok.Data;

import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/22 17:56
 */
@Data
public class HdfsConnectParam {

    private String url;

    /**
     * 用户名
     */
    private String username;

    /**
     * configuration
     */
    private Map<String, String> configuration;
}
