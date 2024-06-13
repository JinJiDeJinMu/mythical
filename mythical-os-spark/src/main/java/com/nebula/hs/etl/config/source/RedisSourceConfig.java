package com.nebula.hs.etl.config.source;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class RedisSourceConfig extends EtlConfig {
    private String host;

    private String port;

    private String redisDb = "0";
    private String keysPattern;
    private String auth = "password";
    private String inferSchema = "true";

    private String sql;

    private String fetchsize = "1024";
    private String readMode = "full";
    private String sourceColumns;

    private String offsetColumns;

    public static void main(String[] args) {
        RedisSourceConfig redisSourceConfig = new RedisSourceConfig();
        redisSourceConfig.setHost("1");
        redisSourceConfig.setPort("2");
        redisSourceConfig.setRedisDb("3");
        redisSourceConfig.setKeysPattern("4");
        redisSourceConfig.setAuth("5");
        redisSourceConfig.setInferSchema("6");
        redisSourceConfig.setSql("7");
        redisSourceConfig.setFetchsize("8");
        redisSourceConfig.setReadMode("9");
        redisSourceConfig.setSourceColumns("10");
        redisSourceConfig.setOffsetColumns("11");

        System.out.println(redisSourceConfig);

        System.out.println(new Gson().toJson(redisSourceConfig.getConfigMap()));
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getRedisDb() {
        return redisDb;
    }

    public void setRedisDb(String redisDb) {
        this.redisDb = redisDb;
    }

    public String getKeysPattern() {
        return keysPattern;
    }

    public void setKeysPattern(String keysPattern) {
        this.keysPattern = keysPattern;
    }

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    public String getInferSchema() {
        return inferSchema;
    }

    public void setInferSchema(String inferSchema) {
        this.inferSchema = inferSchema;
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
        configMap.put("host", host);
        configMap.put("port", port);
        configMap.put("redisDb", redisDb);
        configMap.put("keysPattern", keysPattern);
        configMap.put("auth", auth);
        configMap.put("inferSchema", inferSchema);
        configMap.put("sql", sql);
        configMap.put("fetchsize", fetchsize);
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
                "\"host\":\"" + host + '\"' +
                ", \"port\":\"" + port + '\"' +
                ", \"redisDb\":\"" + redisDb + '\"' +
                ", \"keysPattern\":\"" + keysPattern + '\"' +
                ", \"auth\":\"" + auth + '\"' +
                ", \"inferSchema\":\"" + inferSchema + '\"' +
                ", \"sql\":\"" + sql + '\"' +
                ", \"fetchsize\":\"" + fetchsize + '\"' +
                ", \"readMode\":\"" + readMode + '\"' +
                ", \"sourceColumns\":\"" + sourceColumns + '\"' +
                ", \"offsetColumns\":\"" + offsetColumns + '\"' +
                '}';
    }
}
