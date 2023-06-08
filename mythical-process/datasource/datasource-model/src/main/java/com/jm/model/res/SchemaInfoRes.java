package com.jm.model.res;

import lombok.Data;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/4/13 11:15
 */
@Data
public class SchemaInfoRes {

    private String schemaName;

    /**
     * 是否是默认Schema true - 默认 false - 非默认
     */
    private Boolean current = false;
}
