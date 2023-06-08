package com.jm.storage.plugins.hdfs;


import com.jm.storage.core.spi.AbstractFileSystemClient;
import com.jm.storage.core.spi.model.FileMetaInfo;
import com.jm.storage.core.spi.model.UploadResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/22 17:54
 */
@Slf4j
public class HdfsClient extends AbstractFileSystemClient<HdfsConnectParam> {

    private FileSystem fileSystem;


    public HdfsClient(HdfsConnectParam hdfsConnectParam){
        super(hdfsConnectParam);
    }
    public HdfsClient(String hdfsConnectParam){
        super(hdfsConnectParam);
    }

    @Override
    public Class<HdfsConnectParam> connectParamType() {
        return HdfsConnectParam.class;
    }

    @Override
    protected void init() {
        try {
            Configuration conf = new Configuration();
            if (MapUtils.isNotEmpty(getConnectParam().getConfiguration())) {
                for (Map.Entry<String, String> entry : getConnectParam().getConfiguration().entrySet()) {
                    conf.set(entry.getKey(), entry.getValue());
                }
            }
            fileSystem = FileSystem.get(new URI(getConnectParam().getUrl()), conf, getConnectParam().getUsername());
        } catch (Exception e) {
            throw new RuntimeException("init fail", e);
        }
    }


    @Override
    public boolean connect() {
        try {
            FsStatus status = fileSystem.getStatus();
            return Objects.nonNull(status);
        } catch (IOException e) {
            log.error("connect fail, " + e.getMessage());
        }
        return false;
    }

    @Override
    public List<FileMetaInfo> ls(String path) {
        List<FileMetaInfo> fileMetaInfos = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(path), false);
            while (listFiles.hasNext()){
                LocatedFileStatus fileStatus = listFiles.next();
                fileMetaInfos.add(toFileMetaInfo(fileStatus));
            }
        }catch (Exception e){
            log.error("ls fail, " + e.getMessage());
        }
        return fileMetaInfos;
    }

    @Override
    public FileMetaInfo get(String path) {
        try {
            FileStatus fileStatus = fileSystem.getFileStatus(new Path(path));
            return toFileMetaInfo(fileStatus);
        }catch (Exception e){
            log.error("get fail, " + e.getMessage());
        }
        return null;
    }

    @Override
    public boolean exists(String path) {
        try {
            return fileSystem.exists(new Path(path));
        } catch (IOException e) {
            log.error("exists fail, " + e.getMessage());
        }
        return false;
    }

    @Override
    public boolean mkdir(String path) {
        try {
            return fileSystem.mkdirs(new Path(path));
        } catch (IOException e) {
            log.error("mkdir fail, " + e.getMessage());
        }
        return false;
    }

    @Override
    public UploadResult upload(String targetPath, InputStream sourceStream) {
        Path distFilePath = new Path(targetPath);
        try (FSDataOutputStream fsDataOutputStream = this.fileSystem.create(distFilePath, true);) {
            int size = IOUtils.copy(sourceStream, fsDataOutputStream);

            UploadResult uploadResult = new UploadResult();
            uploadResult.setKey(targetPath);
            uploadResult.setName(extractFileName(targetPath));
            uploadResult.setPath(targetPath);
            uploadResult.setUrl(toUrl(targetPath));
            uploadResult.setSize(size);
            return uploadResult;
        } catch (IOException e) {
            log.error("upload fail, " + e.getMessage());
        }
        return null;
    }

    @Override
    public UploadResult upload(String targetPath, byte[] sourceBytes) {
        return upload(targetPath, new ByteArrayInputStream(sourceBytes));
    }

    @Override
    public InputStream download(String path) {
        try {
           return fileSystem.open(new Path(path));
        }catch (Exception e){
            log.error("download fail, " + e.getMessage());
        }
        return null;
    }

    @Override
    public boolean delete(String path) {
        try {
           return fileSystem.delete(new Path(path),true);
        }catch (Exception e){
            log.error("delete fail, " + e.getMessage());
        }
        return false;
    }

    @Override
    public void close() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            log.error("close fail", e);
        }
    }

    private String toUrl(String targetPath) {
        return getConnectParam().getUrl() + targetPath;
    }

    private FileMetaInfo toFileMetaInfo(FileStatus fileStatus){
        FileMetaInfo fileMetaInfo = new FileMetaInfo();
        fileMetaInfo.setName(fileStatus.getPath().getName());
        fileMetaInfo.setPath(fileStatus.getPath().toUri().getPath());
        fileMetaInfo.setSize(fileStatus.getLen());
        fileMetaInfo.setType(fileStatus.isDirectory() ? fileMetaInfo.DIRECTORY : fileMetaInfo.FILE);
        fileMetaInfo.setUrl(fileStatus.getPath().toString());

        return fileMetaInfo;
    }
}
