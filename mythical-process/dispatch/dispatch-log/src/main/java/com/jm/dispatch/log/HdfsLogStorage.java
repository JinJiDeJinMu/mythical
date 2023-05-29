package com.jm.dispatch.log;


import com.jm.dispatch.log.model.LogResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/26 16:52
 */
public class HdfsLogStorage implements LogStorage{

    private static final Logger LOG = LoggerFactory.getLogger(HdfsLogStorage.class);
    String HdfsPath = "hdfs://192.168.110.42:8020";
    FileSystem fs;


    public void init(){
        try {
            if(fs == null){
                fs = FileSystem.get(new URI(HdfsPath),new Configuration());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(String filePath, String message) throws IOException {
        init();
        FSDataOutputStream fos = null;
        try {
            Path path = new Path(filePath);
            fos = !fs.exists(path) ? fs.create(path) : fs.append(path);
            fos.write(message.getBytes(StandardCharsets.UTF_8));
        }catch (Exception e){
            LOG.error(e.getMessage());
        }finally {
            fos.close();
            fs.close();
        }
    }

    @Override
    public LogResult currentLineRead(String filePath, Long currentLine, Long limit) throws IOException {
        FSDataInputStream fis = null;
        BufferedReader buf = null;
        Long offset = null;
        StringBuilder builder = new StringBuilder();
        try {
            init();
            Path path = new Path(filePath);
            if(fs.exists(path)){
                FileStatus fileStatus = fs.getFileStatus(path);
                fis = fs.open(path);
                long pos = fileStatus.getLen();
                if(currentLine.longValue()>pos){
                    throw new RemoteException("offset超过当前文件最大值");
                }else {
                    fis.skipBytes(currentLine.intValue());
                    buf = new BufferedReader(new InputStreamReader(fis));
                    offset = currentLine;
                    String rawLine;
                    int count =0;
                    while(count++ < limit && (rawLine = buf.readLine()) != null) {
                        String line = rawLine;
                        line = line + "\n";
                        offset += rawLine.getBytes().length + 1;
                        builder.append(line);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            buf.close();
            fis.close();
            fs.close();
        }
        return new LogResult(offset, builder.toString());
    }

    @Override
    public LogResult fullRead(String filePath) throws IOException {
        FSDataInputStream fis = null;
        BufferedReader buf = null;
        StringBuilder builder = new StringBuilder();
        try {
            init();
            Path path = new Path(filePath);
            if(fs.exists(path)){
                fis = fs.open(path);
                buf = new BufferedReader(new InputStreamReader(fis));
                String rawLine;
                while((rawLine = buf.readLine()) != null) {
                    String line = rawLine;
                    line = line + "\n";
                    builder.append(line);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            buf.close();
            fis.close();
            fs.close();
        }
        return new LogResult(-1l, builder.toString());
    }
}
