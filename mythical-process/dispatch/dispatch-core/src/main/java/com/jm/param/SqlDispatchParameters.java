package com.jm.param;

import lombok.Data;

import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/17 20:39
 */
@Data
public class SqlDispatchParameters extends DispatchParameters{

    private String url;

    private String host;

    private Integer port;

    private String database;

    private String username;

    private String password;

    private String driverClassName;

    private Map<String, String> properties;

    @Override
    public boolean checkParameters() {
        return false;
    }
}
