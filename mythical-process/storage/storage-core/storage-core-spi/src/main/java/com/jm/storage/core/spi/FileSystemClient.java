package com.jm.storage.core.spi;



import com.jm.storage.core.spi.model.FileMetaInfo;
import com.jm.storage.core.spi.model.UploadResult;

import java.io.InputStream;
import java.util.List;

/**
 * TODO 文件系统通用接口
 * @Author jinmu
 * @Date 2023/3/22 17:25
 */
public interface FileSystemClient {


    /**
     * 测试连通性
     *
     * @return
     */
    boolean connect();

    /**
     * 查询文件列表
     *
     * @param path
     * @return
     */
    List<FileMetaInfo> ls(String path);

    /**
     * 获取文件
     *
     * @param path
     * @return
     */
    FileMetaInfo get(String path);

    /**
     * 检查文件是否存在
     *
     * @param path
     * @return
     */
    boolean exists(String path);

    /**
     * 创建目录
     *
     * @param path
     * @return
     */
    boolean mkdir(String path);

    /**
     * 上传文件
     *
     * @param targetPath   上传指定路径
     * @param sourceStream 来源数据流
     * @return
     */
    UploadResult upload(String targetPath, InputStream sourceStream);

    /**
     * 上传文件
     *
     * @param targetPath  上传指定路径
     * @param sourceBytes 来源字节数组
     * @return
     */
    UploadResult upload(String targetPath, byte[] sourceBytes);


    /**
     * 文件合并上传
     *
     * @param targetPath
     * @param fsPaths
     * @return
     */
    UploadResult mergeUpload(String targetPath, String... fsPaths);

    /**
     * 本地文件合并上传
     *
     * @param targetPath
     * @param localPaths
     * @return
     */
    UploadResult localMergeUpload(String targetPath, String... localPaths);

    /**
     * 追加文件
     *
     * @param targetPath
     * @param sourceStream
     * @return
     */
    UploadResult append(String targetPath, InputStream sourceStream);


    /**
     * 下载文件
     *
     * @param path
     * @return io流，使用完成必须close
     */
    InputStream download(String path);

    /**
     * 删除文件
     *
     * @param path
     * @return
     */
    boolean delete(String path);

    /**
     * 读取表头
     *
     * @param path
     * @return
     */
    String readHead(String path);

    /**
     * 关闭文件系统客户端
     */
    void close();

}
