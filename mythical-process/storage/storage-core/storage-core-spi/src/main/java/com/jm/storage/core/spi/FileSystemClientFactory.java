package com.jm.storage.core.spi;

/**
 * TODO fileSystemsClient 工厂
 * @Author jinmu
 * @Date 2023/3/22 17:47
 */
public interface FileSystemClientFactory <A extends FileSystemClient,T>{

    /**
     * 支持的类型
     *
     * @return
     */
    String supportType();

    /**
     * 创建客户端
     *
     * @param connectParamJson
     * @return
     */
    A create(String connectParamJson);

    /**
     * 创建客户端
     *
     * @param connectParam
     * @return
     */
    A create(T connectParam);
}
