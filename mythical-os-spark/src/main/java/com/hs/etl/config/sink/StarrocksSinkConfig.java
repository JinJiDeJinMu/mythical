package com.hs.etl.config.sink;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class StarrocksSinkConfig extends EtlConfig {
    private String jdbcUrl;

    private String httpUrl;

    private String username;

    private String password;

    private String targetTable;
    private String batchsize = "1024";

    private String writeMode;

    private String mergeKeys;

    public static void main(String[] args) {
        StarrocksSinkConfig starrocksSinkConfig = new StarrocksSinkConfig();
        starrocksSinkConfig.setJdbcUrl("1");
        starrocksSinkConfig.setHttpUrl("2");
        starrocksSinkConfig.setUsername("3");
        starrocksSinkConfig.setPassword("4");
        starrocksSinkConfig.setTargetTable("5");
        starrocksSinkConfig.setBatchsize("6");
        starrocksSinkConfig.setWriteMode("7");
        starrocksSinkConfig.setMergeKeys("8");

        System.out.println(starrocksSinkConfig);

        System.out.println(new Gson().toJson(starrocksSinkConfig.getConfigMap()));
    }

    public String getHttpUrl() {
        return httpUrl;
    }

    public void setHttpUrl(String httpUrl) {
        this.httpUrl = httpUrl;
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

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
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

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return configMap
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("jdbcUrl", jdbcUrl);
        configMap.put("httpUrl", httpUrl);
        configMap.put("username", username);
        configMap.put("password", password);
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
                "\"jdbcUrl\":\"" + jdbcUrl + '\"' +
                ", \"httpUrl\":\"" + httpUrl + '\"' +
                ", \"username\":\"" + username + '\"' +
                ", \"password\":\"" + password + '\"' +
                ", \"targetTable\":\"" + targetTable + '\"' +
                ", \"batchsize\":\"" + batchsize + '\"' +
                ", \"writeMode\":\"" + writeMode + '\"' +
                ", \"mergeKeys\":\"" + mergeKeys + '\"' +
                '}';
    }
}
