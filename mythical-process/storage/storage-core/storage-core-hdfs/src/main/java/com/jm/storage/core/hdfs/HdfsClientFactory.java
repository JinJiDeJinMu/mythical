package com.jm.storage.core.hdfs;


import com.jm.storage.core.spi.FileSystemClientFactory;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/22 17:54
 */
public class HdfsClientFactory implements FileSystemClientFactory<HdfsClient, HdfsConnectParam> {
    @Override
    public String supportType() {
        return "hdfs";
    }

    @Override
    public HdfsClient create(String connectParamJson) {
        return new HdfsClient(connectParamJson);
    }

    @Override
    public HdfsClient create(HdfsConnectParam connectParam) {
        return new HdfsClient(connectParam);
    }
}
