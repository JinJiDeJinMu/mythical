package com.hs.etl.config.source;

import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class FtpSourceConfig extends EtlConfig {
    private String url;

    private int port;

    private String username;

    private String password;

    private String filePath;

    private String fileType;

    private String nullValue;

    private String delimiter = ",";

    private String encoding = "utf-8";

    private String compressionType;

    private String includeHeader;
    private String sourceColumns;

    private String offsetColumns;

    public String getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(String sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    public String getOffsetColumns() {
        return offsetColumns;
    }

    public void setOffsetColumns(String offsetColumns) {
        this.offsetColumns = offsetColumns;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getNullValue() {
        return nullValue;
    }

    public void setNullValue(String nullValue) {
        this.nullValue = nullValue;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public String getIncludeHeader() {
        return includeHeader;
    }

    public void setIncludeHeader(String includeHeader) {
        this.includeHeader = includeHeader;
    }

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return JsonString
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("filePath", filePath);
        configMap.put("url", url);
        configMap.put("port", port);
        configMap.put("username", username);
        configMap.put("password", password);
        configMap.put("fileType", fileType);
        configMap.put("nullValue", nullValue);
        configMap.put("delimiter", delimiter);
        configMap.put("encoding", encoding);
        configMap.put("compressionType", compressionType);
        configMap.put("includeHeader", includeHeader);
        configMap.put("sourceColumns", sourceColumns);
        configMap.put("offsetColumns", offsetColumns);

        return configMap;
    }

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return JsonString
     */
    @Override
    public String toString() {
        return "{" +
                "\"filePath\":\"" + filePath + '\"' +
                ", \"url\":\"" + url + '\"' +
                ", \"port\":\"" + port + '\"' +
                ", \"username\":\"" + username + '\"' +
                ", \"password\":\"" + password + '\"' +
                ", \"fileType\":\"" + fileType + '\"' +
                ", \"nullValue\":\"" + nullValue + '\"' +
                ", \"delimiter\":\"" + delimiter + '\"' +
                ", \"encoding\":\"" + encoding + '\"' +
                ", \"compressionType\":\"" + compressionType + '\"' +
                ", \"includeHeader\":\"" + includeHeader + '\"' +
                ", \"sourceColumns\":\"" + sourceColumns + '\"' +
                ", \"offsetColumns\":\"" + offsetColumns + '\"' +
                '}';
    }

}
