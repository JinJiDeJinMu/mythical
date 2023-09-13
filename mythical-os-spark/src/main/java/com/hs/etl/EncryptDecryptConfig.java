package com.hs.etl;

import java.io.Serializable;
import java.util.List;

/**
 * @author wzw
 * @description
 * @date 2023/6/30
 */
public class EncryptDecryptConfig implements Serializable {
    private String tableName;

    private String colName;

    private String algName;

    private List<Object> algParams;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getAlgName() {
        return algName;
    }

    public void setAlgName(String algName) {
        this.algName = algName;
    }

    public List<Object> getAlgParams() {
        return algParams;
    }

    public void setAlgParams(List<Object> algParams) {
        this.algParams = algParams;
    }

    @Override
    public String toString() {
        return "EncryptDecryptConfig{" +
                "tableName='" + tableName + '\'' +
                ", colName='" + colName + '\'' +
                ", algName='" + algName + '\'' +
                ", algParams=" + algParams +
                '}';
    }
}
