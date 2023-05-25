package com.jm.storage.core.spi.model;


import lombok.Data;

/**
 * TODO 文件元数据
 * @Author jinmu
 * @Date 2023/3/22 17:25
 */
@Data
public class FileMetaInfo {

    public static final String UN_KNOW = "0";
    public static final String DIRECTORY = "1";
    public static final String FILE = "2";

    /**
     * 文件路径
     * /opt/app
     */
    private String path;
    /**
     * 文件名称
     * data_20220808.csv
     */
    private String name;
    /**
     * 文件类型
     * 0-未知 1-目录 2-文件
     */
    private String type;
    /**
     * 文件URL,非必填
     */
    private String url;
    /**
     * 文件大小,单位：byte
     */
    private long size;
}
