package com.hs.etl.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.hs.etl.DataConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class EtlConfig {

    public static Gson gson = new GsonBuilder().create();
    private DataConfig source;
    private DataConfig sink;
    private String taskID;
    private String columnMap;
    private ExtraConfig extraConfig;

    public static EtlConfig getEtlConfig(String params) {
        return gson.fromJson(params, EtlConfig.class);
    }

    public static JsonElement getJsonElement(String params) {
        return gson.fromJson(params, JsonElement.class);
    }

    public ExtraConfig getExtraConfig() {
        return extraConfig;
    }

    public void setExtraConfig(ExtraConfig extraConfig) {
        this.extraConfig = extraConfig;
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    public String getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(String columnMap) {
        this.columnMap = columnMap;
    }

    public DataConfig getSource() {
        return source;
    }

    public void setSource(DataConfig source) {
        this.source = source;
    }

    public DataConfig getSink() {
        return sink;
    }

    public void setSink(DataConfig sink) {
        this.sink = sink;
    }

    /**
     * This method must be overridden in all sourceConfig and sinkConfig !
     *
     * @return configMap
     */
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("taskID", taskID);
        configMap.put("columnMap", columnMap);
        //return new GsonBuilder().disableHtmlEscaping().create().toJson(jsonStr);
        return configMap;
    }
}
