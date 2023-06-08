package com.jm.model.res;

import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/4/13 13:49
 */
@Data
public class TableInfoRes {

    private String catalogName;

    private String databaseName;

    private String tableName;

    private String description;

    private Boolean isPartition;

    private Long storageSize = 0l;

    private Long numRows = 0l;

    private Date createTime;

    private Date updateTime;

    private String owner;

    private List<ColumnInfoRes> columnInfoList;

    private List<PartitionInfoRes> partitionInfoList;
}
