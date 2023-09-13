package com.hs.etl.config.source;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class MongoSourceConfig extends EtlConfig {
    private String url;

    private String sourceDatabase;

    private String collection;

    private String sql;

    private String fetchsize = "1024";
    private String readMode = "full";
    private String sourceColumns;

    private String offsetColumns;

    public static void main(String[] args) {
        MongoSourceConfig mongoSourceConfig = new MongoSourceConfig();
        mongoSourceConfig.setUrl("1");
        mongoSourceConfig.setSourceDatabase("2");
        mongoSourceConfig.setCollection("3");
        mongoSourceConfig.setSql("4");
        mongoSourceConfig.setFetchsize("5");
        mongoSourceConfig.setReadMode("6");
        mongoSourceConfig.setSourceColumns("7");
        mongoSourceConfig.setOffsetColumns("8");

        System.out.println(mongoSourceConfig);

        System.out.println(new Gson().toJson(mongoSourceConfig.getConfigMap()));
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

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

    public String getFetchsize() {
        return fetchsize;
    }

    public void setFetchsize(String fetchsize) {
        this.fetchsize = fetchsize;
    }

    public String getReadMode() {
        return readMode;
    }

    public void setReadMode(String readMode) {
        this.readMode = readMode;
    }

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return configMap
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", url);
        configMap.put("sourceDatabase", sourceDatabase);
        configMap.put("collection", collection);
        configMap.put("sql", sql);
        configMap.put("fetchsize", fetchsize);
        configMap.put("readMode", readMode);
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
                "\"url\":\"" + url + '\"' +
                ", \"sourceDatabase\":\"" + sourceDatabase + '\"' +
                ", \"collection\":\"" + collection + '\"' +
                ", \"sql\":\"" + sql + '\"' +
                ", \"fetchsize\":\"" + fetchsize + '\"' +
                ", \"readMode\":\"" + readMode + '\"' +
                ", \"sourceColumns\":\"" + sourceColumns + '\"' +
                ", \"offsetColumns\":\"" + offsetColumns + '\"' +
                '}';
    }
}
