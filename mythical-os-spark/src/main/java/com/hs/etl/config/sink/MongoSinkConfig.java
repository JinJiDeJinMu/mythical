package com.hs.etl.config.sink;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class MongoSinkConfig extends EtlConfig {
    private String url;

    private String collection;

    private String targetDatabase;

    private String batchsize = "1024";

    private String writeMode;

    private String mergeKeys;

    public static void main(String[] args) {
        MongoSinkConfig mongoSinkConfig = new MongoSinkConfig();
        mongoSinkConfig.setUrl("1");
        mongoSinkConfig.setCollection("2");
        mongoSinkConfig.setTargetDatabase("3");
        mongoSinkConfig.setBatchsize("4");
        mongoSinkConfig.setWriteMode("5");
        mongoSinkConfig.setMergeKeys("6");

        System.out.println(mongoSinkConfig);

        System.out.println(new Gson().toJson(mongoSinkConfig.getConfigMap()));
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getMergeKeys() {
        return mergeKeys;
    }

    public void setMergeKeys(String mergeKeys) {
        this.mergeKeys = mergeKeys;
    }

    public String getBatchsize() {
        return batchsize;
    }

    public void setBatchsize(String batchsize) {
        this.batchsize = batchsize;
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
        configMap.put("collection", collection);
        configMap.put("targetDatabase", targetDatabase);
        configMap.put("batchsize", batchsize);
        configMap.put("writeMode", writeMode);
        configMap.put("mergeKeys", mergeKeys);

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
                ", \"collection\":\"" + collection + '\"' +
                ", \"targetDatabase\":\"" + targetDatabase + '\"' +
                ", \"batchsize\":\"" + batchsize + '\"' +
                ", \"writeMode\":\"" + writeMode + '\"' +
                ", \"mergeKeys\":\"" + mergeKeys + '\"' +
                '}';
    }
}
