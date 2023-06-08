package com.jm.storage.core.spi;

import cn.hutool.json.JSONUtil;
import com.jm.storage.core.spi.model.UploadResult;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/22 17:35
 */
public abstract class AbstractFileSystemClient<T> implements FileSystemClient {

    T connectParam;

    public AbstractFileSystemClient(String connectParamJson){
        this.connectParam = JSONUtil.toBean(connectParamJson,connectParamType());
        init();
    }

    public AbstractFileSystemClient(T connectParam) {
        this.connectParam = connectParam;
        init();
    }

    public T getConnectParam() {
        return connectParam;
    }

    /**
     * 获取连接参数类型
     *
     * @return
     */
    public abstract Class<T> connectParamType();

    /**
     * 初始化连接
     */
    protected abstract void init();

    @Override
    public UploadResult mergeUpload(String targetPath, String... fsPaths) {
        throw new UnsupportedOperationException("not support merge upload");
    }

    @Override
    public UploadResult localMergeUpload(String targetPath, String... localPaths) {
        throw new UnsupportedOperationException("not support local merge upload");
    }

    @Override
    public UploadResult append(String targetPath, InputStream sourceStream) {
        throw new UnsupportedOperationException("not support append");
    }

    @Override
    public String readHead(String path) {
        throw new UnsupportedOperationException("not support readHead");
    }

    protected String extractFileName(String path) {
        if (StringUtils.isBlank(path)) {
            return "";
        }
        int index = path.lastIndexOf("/");
        if (index > 0) {
            return path.substring(index + 1);
        }
        return path;
    }

    /**
     * 提取目录
     * a => null
     * /a/b/c => /a/b
     * /a/b/c/a.txt => /a/b/c
     *
     * @param path
     * @return
     */
    protected String extractDir(String path) {
        if (StringUtils.isBlank(path)) {
            return "";
        }
        int index = path.lastIndexOf("/");
        if (index > 0) {
            return path.substring(0, index);
        }
        return null;
    }
}
