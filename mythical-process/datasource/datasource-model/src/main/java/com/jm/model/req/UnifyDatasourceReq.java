package com.jm.model.req;

import lombok.Data;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/7 11:30
 */
@Data
public class UnifyDatasourceReq extends DatasourceBaseReq {

    private String schemaName;

    private String databaseName;

    private String tableName;

}
