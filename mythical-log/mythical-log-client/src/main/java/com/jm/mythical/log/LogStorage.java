package com.jm.mythical.log;

import com.jm.mythical.log.model.LogResult;

import java.io.IOException;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/26 14:28
 */
public interface LogStorage {

    void write(String filePath, List<String> messages) throws IOException;

    LogResult currentLineRead(String filePath, Long currentLine, Long limit) throws IOException;

    LogResult fullRead(String filePath) throws IOException;
}
