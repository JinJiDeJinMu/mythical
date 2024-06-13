package com.nebula.hs.etl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wzw
 * @Description
 * @date 2023/7/3
 */
public class PartitionsConfig implements Serializable {
    private String partitionType;

    private String partitionSource;

    private String partitionTarget;

    private String partitionFormat = "unChange";

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public String getPartitionSource() {
        return partitionSource;
    }

    public void setPartitionSource(String partitionSource) {
        this.partitionSource = partitionSource;
    }

    public String getPartitionTarget() {
        return partitionTarget;
    }

    public void setPartitionTarget(String partitionTarget) {
        this.partitionTarget = partitionTarget;
    }

    public String getPartitionFormat() {
        return partitionFormat;
    }

    public void setPartitionFormat(String partitionFormat) {
        this.partitionFormat = partitionFormat;
    }


    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return configMap
     */
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("partitionType", partitionType);
        configMap.put("partitionSource", partitionSource);
        configMap.put("partitionTarget", partitionTarget);
        configMap.put("partitionFormat", partitionFormat);
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
                "\"partitionType\":\"" + partitionType + '\"' +
                ", \"partitionSource\":\"" + partitionSource + '\"' +
                ", \"partitionTarget\":\"" + partitionTarget + '\"' +
                ", \"partitionFormat\":\"" + partitionFormat + '\"' +
                '}';
    }
}
