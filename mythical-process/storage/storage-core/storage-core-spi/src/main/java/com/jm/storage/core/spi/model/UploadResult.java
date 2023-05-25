package com.jm.storage.core.spi.model;

import lombok.Data;

/**
 * TODO 文件上传结果
 * @Author jinmu
 * @Date 2023/3/22 17:25
 */
@Data
public class UploadResult {

    /**
     * 文件key 必填
     */
    private String key;

    /**
     * 文件路径
     */
    private String path;

    /**
     * 文件名称
     */
    private String name;

    /**
     * 文件大小
     */
    private long size;

    /**
     * 文件访问url，非必填
     */
    private String url;
}
