package com.jm.model.req;

import lombok.Data;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/8 15:20
 */
@Data
public class UnifyTableReq {

    private String sql;

    /**
     * schema/db
     */
    private String schema;

    /**
     * 表名称
     */
    private String tableName;

    /**
     * 表名称(正则)
     */
    private String tableNamePattern;

}
