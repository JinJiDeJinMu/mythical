package com.hs.etl.config.source;

import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class CsvSourceConfig extends EtlConfig {
    private String filePath;

    private String includeHeader = "true";

    private String delimiter = ";";

    private String encoding = "utf-8";

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

    public String getIncludeHeader() {
        return includeHeader;
    }

    public void setIncludeHeader(String includeHeader) {
        this.includeHeader = includeHeader;
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

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return JsonString
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("filePath", filePath);
        configMap.put("includeHeader", includeHeader);
        configMap.put("delimiter", delimiter);
        configMap.put("encoding", encoding);
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
                ", \"includeHeader\":\"" + includeHeader + '\"' +
                ", \"delimiter\":\"" + delimiter + '\"' +
                ", \"encoding\":\"" + encoding + '\"' +
                ", \"sourceColumns\":\"" + sourceColumns + '\"' +
                ", \"offsetColumns\":\"" + offsetColumns + '\"' +
                '}';
    }

}
