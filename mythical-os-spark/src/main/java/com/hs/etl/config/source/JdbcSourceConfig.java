package com.hs.etl.config.source;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class JdbcSourceConfig extends EtlConfig {
    private String jdbcUrl;

    private String username;

    private String password;

    private String query;

    private String fetchsize = "1024";
    private String readMode = "full";
    private String sourceColumns;

    private String offsetColumns;

    public static void main(String[] args) {
        JdbcSourceConfig jdbcSourceConfig = new JdbcSourceConfig();
        jdbcSourceConfig.setJdbcUrl("1");
        jdbcSourceConfig.setUsername("2");
        jdbcSourceConfig.setPassword("3");
        jdbcSourceConfig.setQuery("4");
        jdbcSourceConfig.setFetchsize("5");
        jdbcSourceConfig.setReadMode("6");
        jdbcSourceConfig.setSourceColumns("7");
        jdbcSourceConfig.setOffsetColumns("8");

        System.out.println(jdbcSourceConfig);

        System.out.println(new Gson().toJson(jdbcSourceConfig.getConfigMap()));
    }

    public String getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(String sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    public String getOffsetColumns() {
        return offsetColumns;
    }

    public void setOffsetColumns(String offsetColumns) {
        this.offsetColumns = offsetColumns;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getFetchsize() {
        return fetchsize;
    }

    public void setFetchsize(String fetchsize) {
        this.fetchsize = fetchsize;
    }

    public String getReadMode() {
        return readMode;
    }

    public void setReadMode(String readMode) {
        this.readMode = readMode;
    }

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return configMap
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("jdbcUrl", jdbcUrl);
        configMap.put("username", username);
        configMap.put("password", password);
        configMap.put("fetchsize", fetchsize);
        configMap.put("query", query);
        configMap.put("readMode", readMode);
        configMap.put("sourceColumns", sourceColumns);
        configMap.put("offsetColumns", offsetColumns);

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
                "\"jdbcUrl\":\"" + jdbcUrl + '\"' +
                ", \"username\":\"" + username + '\"' +
                ", \"password\":\"" + password + '\"' +
                ", \"query\":\"" + query + '\"' +
                ", \"fetchsize\":\"" + fetchsize + '\"' +
                ", \"readMode\":\"" + readMode + '\"' +
                ", \"sourceColumns\":\"" + sourceColumns + '\"' +
                ", \"offsetColumns\":\"" + offsetColumns + '\"' +
                '}';
    }
}
