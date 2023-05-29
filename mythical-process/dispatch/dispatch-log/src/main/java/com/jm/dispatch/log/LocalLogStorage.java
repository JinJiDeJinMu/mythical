package com.jm.dispatch.log;


import com.jm.dispatch.log.model.LogResult;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/26 15:51
 */
public class LocalLogStorage implements LogStorage {

    private static final String ISO_CHARACTER = "ISO-8859-1";

    private static final String ENCODING = "utf-8";

    @Override
    public void write(String filePath, List<String> messages) throws IOException {
        if(!new File(filePath).exists()){
            throw new IOException("文件不存在");
        }
        FileWriter writer = new FileWriter(filePath, true);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        for (String message : messages) {
            bufferedWriter.write(message);
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
        writer.close();
    }

    @Override
    public LogResult currentLineRead(String filePath, Long currentLine, Long limit) throws IOException {
        if(!new File(filePath).exists()){
            throw new IOException("文件不存在");
        }
        BufferedRandomAccessFile baf = new BufferedRandomAccessFile(filePath, "r");
        if(currentLine.longValue()>baf.length()){
            throw new IOException("当前offset已经超过文件最大值");
        }else {
            baf.seek(currentLine);
            StringBuilder builder = new StringBuilder();
            Long offset = currentLine;
            String rawLine;
            int count =0;
            while(count++ < limit && (rawLine = baf.readLine()) != null) {
                String line = new String(rawLine.getBytes(ISO_CHARACTER), ENCODING);

                line = line + "\n";
                offset += rawLine.length() + 1 ;
                builder.append(line);
            }
            return new LogResult(offset, builder.toString());
        }
    }

    @Override
    public LogResult fullRead(String filePath) throws IOException {
        if(!new File(filePath).exists()){
            throw new IOException("文件不存在");
        }
        BufferedRandomAccessFile baf = new BufferedRandomAccessFile(filePath, "r");
        StringBuilder builder = new StringBuilder();
        String rawLine;
        while((rawLine = baf.readLine()) != null) {
            String line = new String(rawLine.getBytes(ISO_CHARACTER), ENCODING);

            line = line + "\n";
            builder.append(line);
        }

        //todo 流式读取文件
//        String message = Files.lines(Paths.get(filePath))
//                .parallel()
//                .collect(Collectors.joining(System.lineSeparator()));

        return new LogResult(-1l, builder.toString());
    }
}
