package com.jm.model.req;


import lombok.Data;

import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/7 13:42
 */
@Data
public abstract class DatasourceBaseReq {

    protected String username;

    protected String password;

    protected String url;

    protected String datasourceType;

    protected Map<String, Object> extraParams;
}
