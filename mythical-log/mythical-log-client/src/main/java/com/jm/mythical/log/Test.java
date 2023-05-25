package com.jm.mythical.log;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/23 10:04
 */
public class Test {

    public static void main(String[] args) throws IOException {
        FileSystem fs;
        try {
             fs = FileSystem.get(new URI("hdfs://192.168.110.42:8020"),new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        //fs.create(new Path("/jinmu/test/append/1.txt"));
//        FSDataOutputStream fos = fs.append(new Path("/jinmu/test/append/1.txt"));
//        for (int i = 100; i < 200; i++) {
//            String msg = "追加 追加 1追加 2追加 3追加 4追加 5追加 6追加 7追加 8追加 9追加 10追加 追加 " + i + "\n";
//            fos.write(msg.getBytes(StandardCharsets.UTF_8));
//        }

//        FSDataInputStream fis = fs.open(new Path("/jinmu/test/append/1.txt"));
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));

//        ByteArrayOutputStream stream = new ByteArrayOutputStream();
//        IOUtils.copyBytes(fis, stream, 1024);
//        System.out.println(new String(stream.toByteArray()));

        //5290 1030
        //System.out.println("/n".length());

        readLog(fs,"/jinmu/test/append/1.txt",1050,20);


        //readLog("/Users/jinmu/Downloads/1.txt",12610,120);
    }
    public static void readLog(FileSystem fs,String path, Integer currentLine, Integer limit){
        try {
            Path filePath = new Path(path);
            if(fs.exists(filePath)){
                FileStatus fileStatus = fs.getFileStatus(filePath);
                FSDataInputStream fis = fs.open(filePath);
                long pos = fileStatus.getLen();
                if(currentLine.longValue()>pos){
                    System.out.println("当前offset已经超过文件最大值");
                }else {
                    fis.skipBytes(currentLine);
                    BufferedReader buf = new BufferedReader(new InputStreamReader(fis));
                    StringBuilder builder = new StringBuilder();
                    int offset = currentLine;
                    String rawLine;
                    int count =0;
                    while(count++ < limit && (rawLine = buf.readLine()) != null) {
                        //String line = new String(rawLine.getBytes("ISO-8859-1"), "utf-8");

                        String line = rawLine;
                        line = line + "\n";
                        offset += rawLine.length() +1;
                        builder.append(line);
                    }
                    System.out.println("当前位置" + fis.getPos());
                    System.out.println("当前offset = " + offset);
                    System.out.println(builder.toString());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void readLog(String path, Integer currentLine, Integer limit){
        try {
            File file = new File(path);

            if(!file.exists()){
                System.out.println("文件不存在");
                return;
            }
            BufferedRandomAccessFile baf = new BufferedRandomAccessFile(path, "r");
                if(currentLine.longValue()>baf.length()){
                    System.out.println("当前offset已经超过文件最大值");
                }else {
                    baf.seek(currentLine);

                    StringBuilder builder = new StringBuilder();
                    int offset = currentLine;
                    String rawLine;
                    int count =0;
                    while(count++ < limit && (rawLine = baf.readLine()) != null) {
                        String line = new String(rawLine.getBytes("ISO-8859-1"), "utf-8");

                        line = line + "\n";
                        offset += rawLine.length() + 1 ;
                        builder.append(line);
                    }
                    System.out.println("当前offset = " + offset);
                    System.out.println(builder.toString());
                }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
