package com.nebula.hs.etl;

import java.io.Serializable;

/**
 * @author wzw
 * @Description
 * @date 2023/7/5
 */
public class WatermarkConfig implements Serializable {
    private String tableName = "";

    private String isWatermark = "0";

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getIsWatermark() {
        return isWatermark;
    }

    public void setIsWatermark(String isWatermark) {
        this.isWatermark = isWatermark;
    }

    @Override
    public String toString() {
        return "WatermarkConfig{" +
                "tableName='" + tableName + '\'' +
                ", isWatermark='" + isWatermark + '\'' +
                '}';
    }
}
