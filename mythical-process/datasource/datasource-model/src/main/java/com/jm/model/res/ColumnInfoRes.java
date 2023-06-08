package com.jm.model.res;

import lombok.Data;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/4/13 13:50
 */
@Data
public class ColumnInfoRes {

    private Integer index;

    private String columnName;

    private String type;

    private String comment;

    private Boolean isPartitionField;

    private Boolean isPrimaryKey;

    private Boolean isNullable;

}
