package com.hs.etl.config.sink;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class EsSinkConfig extends EtlConfig {

    private String username;

    private String password;

    private String targetTable;

    private String esNodes;

    private String esIndex;

    private String esConf = "";

    private String esType = "_doc";
    private String batchsize = "1024";

    private String writeMode;

    private String mergeKeys;

    private String esJsonColumns;

    public static void main(String[] args) {
        EsSinkConfig esSinkConfig = new EsSinkConfig();
        esSinkConfig.setUsername("1");
        esSinkConfig.setPassword("2");
        esSinkConfig.setTargetTable("3");
        esSinkConfig.setEsNodes("4");
        esSinkConfig.setEsIndex("5");
        esSinkConfig.setEsConf("6");
        esSinkConfig.setEsType("7");
        esSinkConfig.setBatchsize("8");
        esSinkConfig.setWriteMode("9");
        esSinkConfig.setMergeKeys("10");
        esSinkConfig.setEsJsonColumns("11");

        System.out.println(esSinkConfig);

        System.out.println(new Gson().toJson(esSinkConfig.getConfigMap()));
    }

    public String getEsJsonColumns() {
        return esJsonColumns;
    }

    public void setEsJsonColumns(String esJsonColumns) {
        this.esJsonColumns = esJsonColumns;
    }

    public String getEsNodes() {
        return esNodes;
    }

    public void setEsNodes(String esNodes) {
        this.esNodes = esNodes;
    }

    public String getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }

    public String getEsConf() {
        return esConf;
    }

    public void setEsConf(String esConf) {
        this.esConf = esConf;
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
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
     * @return JsonString
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("username", username);
        configMap.put("password", password);
        configMap.put("targetTable", targetTable);
        configMap.put("esNodes", esNodes);
        configMap.put("esIndex", esIndex);
        configMap.put("esConf", esConf);
        configMap.put("esType", esType);
        configMap.put("batchsize", batchsize);
        configMap.put("writeMode", writeMode);
        configMap.put("mergeKeys", mergeKeys);
        configMap.put("esJsonColumns", esJsonColumns);

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
                "\"username\":\"" + username + '\"' +
                ", \"password\":\"" + password + '\"' +
                ", \"targetTable\":\"" + targetTable + '\"' +
                ", \"esNodes\":\"" + esNodes + '\"' +
                ", \"esIndex\":\"" + esIndex + '\"' +
                ", \"esConf\":\"" + esConf + '\"' +
                ", \"esType\":\"" + esType + '\"' +
                ", \"batchsize\":\"" + batchsize + '\"' +
                ", \"writeMode\":\"" + writeMode + '\"' +
                ", \"mergeKeys\":\"" + mergeKeys + '\"' +
                ", \"esJsonColumns\":\"" + esJsonColumns + '\"' +
                '}';
    }
}
