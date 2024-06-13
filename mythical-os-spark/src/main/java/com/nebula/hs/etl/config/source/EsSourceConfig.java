package com.nebula.hs.etl.config.source;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class EsSourceConfig extends EtlConfig {
    private String jdbcUrl;

    private String username = "";

    private String password = "";

    private String esQuery = "{\"query\":{\"match_all\":{}}}";

    private String esNodes;

    private String esIndex;

    private String esConf = "";

    private String esType = "_doc";

    private String fetchsize = "1024";
    private String readMode = "full";
    private String sourceColumns;

    private String offsetColumns;

    public static void main(String[] args) {
        EsSourceConfig esSourceConfig = new EsSourceConfig();
        esSourceConfig.setJdbcUrl("1");
        esSourceConfig.setUsername("2");
        esSourceConfig.setPassword("3");
        esSourceConfig.setEsQuery("4");
        esSourceConfig.setEsNodes("5");
        esSourceConfig.setEsIndex("6");
        esSourceConfig.setEsConf("7");
        esSourceConfig.setEsType("8");
        esSourceConfig.setFetchsize("9");
        esSourceConfig.setReadMode("10");
        esSourceConfig.setSourceColumns("11");
        esSourceConfig.setOffsetColumns("12");

        System.out.println(esSourceConfig);

        System.out.println(new Gson().toJson(esSourceConfig.getConfigMap()));
    }

    public String getEsConf() {
        return esConf;
    }

    public void setEsConf(String esConf) {
        this.esConf = esConf;
    }

    public String getEsQuery() {
        return esQuery;
    }

    public void setEsQuery(String esQuery) {
        this.esQuery = esQuery;
    }

    public String getEsNodes() {
        return esNodes;
    }

    public void setEsNodes(String esNodes) {
        this.esNodes = esNodes;
    }

    public String getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
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
     * @return JsonString
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("jdbcUrl", jdbcUrl);
        configMap.put("username", username);
        configMap.put("password", password);
        configMap.put("esQuery", esQuery);
        configMap.put("esNodes", esNodes);
        configMap.put("esIndex", esIndex);
        configMap.put("esConf", esConf);
        configMap.put("esType", esType);
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
                "\"jdbcUrl\":\"" + jdbcUrl + '\"' +
                ", \"username\":\"" + username + '\"' +
                ", \"password\":\"" + password + '\"' +
                ", \"esQuery\":\"" + esQuery + '\"' +
                ", \"esNodes\":\"" + esNodes + '\"' +
                ", \"esIndex\":\"" + esIndex + '\"' +
                ", \"esConf\":\"" + esConf + '\"' +
                ", \"esType\":\"" + esType + '\"' +
                ", \"fetchsize\":\"" + fetchsize + '\"' +
                ", \"readMode\":\"" + readMode + '\"' +
                ", \"sourceColumns\":\"" + sourceColumns + '\"' +
                ", \"offsetColumns\":\"" + offsetColumns + '\"' +
                '}';
    }
}
