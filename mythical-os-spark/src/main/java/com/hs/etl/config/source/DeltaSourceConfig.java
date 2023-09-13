package com.hs.etl.config.source;

import com.google.gson.GsonBuilder;
import com.hs.config.SystemConfig;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class DeltaSourceConfig extends EtlConfig {

    private String sourceDatabase;

    private String sourceTable;
    private String query;

    private String tablePathFormat = SystemConfig.tablePathFormat();

    private String fetchsize = "1024";
    private String readMode = "full";
    private String sourceColumns;

    private String offsetColumns;

    public static void main(String[] args) {
        DeltaSourceConfig deltaSourceConfig = new DeltaSourceConfig();
        deltaSourceConfig.setSourceDatabase("1");
        deltaSourceConfig.setSourceTable("2");
        deltaSourceConfig.setQuery("select * from stg.stg_deda_lget_product_cert_rel where subject_type = 'factory'");
        deltaSourceConfig.setTablePathFormat("4");
        deltaSourceConfig.setFetchsize("5");
        deltaSourceConfig.setReadMode("6");
        deltaSourceConfig.setSourceColumns("7");
        deltaSourceConfig.setOffsetColumns("8");

        System.out.println(deltaSourceConfig);

        System.out.println(new GsonBuilder().disableHtmlEscaping().create().toJson(deltaSourceConfig.getConfigMap()));
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

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getTablePathFormat() {
        return tablePathFormat;
    }

    public void setTablePathFormat(String tablePathFormat) {
        this.tablePathFormat = tablePathFormat;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
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
        configMap.put("sourceDatabase", sourceDatabase);
        configMap.put("sourceTable", sourceTable);
        configMap.put("query", query);
        configMap.put("tablePathFormat", tablePathFormat);
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
                "\"sourceDatabase\":\"" + sourceDatabase + '\"' +
                ", \"sourceTable\":\"" + sourceTable + '\"' +
                ", \"query\":\"" + query + '\"' +
                ", \"tablePathFormat\":\"" + tablePathFormat + '\"' +
                ", \"fetchsize\":\"" + fetchsize + '\"' +
                ", \"readMode\":\"" + readMode + '\"' +
                ", \"sourceColumns\":\"" + sourceColumns + '\"' +
                ", \"offsetColumns\":\"" + offsetColumns + '\"' +
                '}';
    }
}
