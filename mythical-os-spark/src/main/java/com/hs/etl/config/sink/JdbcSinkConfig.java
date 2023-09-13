package com.hs.etl.config.sink;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class JdbcSinkConfig extends EtlConfig {
    private String jdbcUrl;

    private String username;

    private String password;

    private String targetTable;
    private String batchsize = "1024";

    private String writeMode;

    private String mergeKeys;

    private String breakpointResumeColumn;

    private String breakpointResume = "false";

    public static void main(String[] args) {
        JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig();
        jdbcSinkConfig.setJdbcUrl("jdbc:postgresql://192.168.200.166:5432/test");
        jdbcSinkConfig.setJdbcUrl("lakerw");
        jdbcSinkConfig.setPassword("cxb6#3Fd&hj6L2dm");
        jdbcSinkConfig.setTargetTable("dm.dm_lget_product_cert_factory_rel");
        jdbcSinkConfig.setBatchsize("1024");
        jdbcSinkConfig.setWriteMode("update");
        jdbcSinkConfig.setMergeKeys("id");

        System.out.println(jdbcSinkConfig);

        //fixme:
        System.out.println(new Gson().toJson(jdbcSinkConfig.getConfigMap()));
    }

    public String getBreakpointResume() {
        return breakpointResume;
    }

    public void setBreakpointResume(String breakpointResume) {
        this.breakpointResume = breakpointResume;
    }

    public String getBreakpointResumeColumn() {
        return breakpointResumeColumn;
    }

    public void setBreakpointResumeColumn(String breakpointResumeColumn) {
        this.breakpointResumeColumn = breakpointResumeColumn;
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
        configMap.put("username", username);
        configMap.put("password", password);
        configMap.put("targetTable", targetTable);
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
                ", \"username\":\"" + username + '\"' +
                ", \"password\":\"" + password + '\"' +
                ", \"targetTable\":\"" + targetTable + '\"' +
                ", \"batchsize\":\"" + batchsize + '\"' +
                ", \"writeMode\":\"" + writeMode + '\"' +
                ", \"mergeKeys\":\"" + mergeKeys + '\"' +
                '}';
    }
}
