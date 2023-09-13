package com.hs.etl.config.sink;

import com.google.gson.Gson;
import com.hs.config.SystemConfig;
import com.hs.etl.PartitionsConfig;
import com.hs.etl.config.EtlConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class DeltaSinkConfig extends EtlConfig {
    private String targetTable;

    private String sortColumns;

    private String targetDatabase = "default";

    private String tablePathFormat = SystemConfig.tablePathFormat();

    private String batchsize = "1024";

    private String writeMode;

    private String mergeKeys;

    private List<PartitionsConfig> partitions = new ArrayList<>();

    public static void main(String[] args) {
        PartitionsConfig par = new PartitionsConfig();
        par.setPartitionType("time");
        par.setPartitionSource("sourCol");
        par.setPartitionTarget("sinkCol");
        par.setPartitionFormat("year");
        PartitionsConfig par1 = new PartitionsConfig();
        par1.setPartitionType("type");
        par1.setPartitionSource("sourCol");
        par1.setPartitionTarget("sinkCol");
        par1.setPartitionFormat("unChange");

        ArrayList<PartitionsConfig> partitionsConfigs = new ArrayList<>();
        partitionsConfigs.add(par);
        partitionsConfigs.add(par1);

        DeltaSinkConfig configs = new DeltaSinkConfig();
        configs.setPartitions(partitionsConfigs);
        System.out.println(configs);

        Map<String, Object> configMap1 = configs.getConfigMap();
        System.out.println(new Gson().toJson(configMap1));
    }

    public List<PartitionsConfig> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<PartitionsConfig> partitions) {
        this.partitions = partitions;
    }

    public String getSortColumns() {
        return sortColumns;
    }

    public void setSortColumns(String sortColumns) {
        this.sortColumns = sortColumns;
    }

    public String getTablePathFormat() {
        return tablePathFormat;
    }

    public void setTablePathFormat(String tablePathFormat) {
        this.tablePathFormat = tablePathFormat;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
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
        configMap.put("targetTable", targetTable);
        configMap.put("sortColumns", sortColumns);
        configMap.put("targetDatabase", targetDatabase);
        configMap.put("tablePathFormat", tablePathFormat);
        configMap.put("batchsize", batchsize);
        configMap.put("writeMode", writeMode);
        configMap.put("mergeKeys", mergeKeys);
        configMap.put("partitions", partitions);

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
                "\"targetTable\":\"" + targetTable + '\"' +
                ", \"sortColumns\":\"" + sortColumns + '\"' +
                ", \"targetDatabase\":\"" + targetDatabase + '\"' +
                ", \"tablePathFormat\":\"" + tablePathFormat + '\"' +
                ", \"batchsize\":\"" + batchsize + '\"' +
                ", \"writeMode\":\"" + writeMode + '\"' +
                ", \"mergeKeys\":\"" + mergeKeys + '\"' +
                ", \"partitions\":" + partitions +
                '}';
    }
}
