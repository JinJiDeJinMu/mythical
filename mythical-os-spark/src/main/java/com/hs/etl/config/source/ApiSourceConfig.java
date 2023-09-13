package com.hs.etl.config.source;

import com.google.gson.Gson;
import com.hs.etl.config.EtlConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/7/13
 */
public class ApiSourceConfig extends EtlConfig {

    private String apiUrl;

    private String method;

    private String dataMode;

    private String dataPath;

    private String responseType;

    private String header;

    private String parameters;

    private String requestTimes;

    private String authType;

    private String username;

    private String password;

    private String token;


    private String sourceColumns;

    private String offsetColumns;

    public static void main(String[] args) {
        ApiSourceConfig apiSourceConfig = new ApiSourceConfig();
        apiSourceConfig.setApiUrl("1");
        apiSourceConfig.setMethod("2");
        apiSourceConfig.setDataMode("3");
        apiSourceConfig.setDataPath("4");
        apiSourceConfig.setResponseType("5");
        apiSourceConfig.setHeader("6");
        apiSourceConfig.setParameters("7");
        apiSourceConfig.setRequestTimes("8");
        apiSourceConfig.setAuthType("9");
        apiSourceConfig.setUsername("10");
        apiSourceConfig.setParameters("11");
        apiSourceConfig.setToken("12");
        apiSourceConfig.setSourceColumns("13");
        apiSourceConfig.setOffsetColumns("14");

        System.out.println(apiSourceConfig);

        System.out.println(new Gson().toJson(apiSourceConfig.getConfigMap()));
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public void setApiUrl(String apiUrl) {
        this.apiUrl = apiUrl;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getDataMode() {
        return dataMode;
    }

    public void setDataMode(String dataMode) {
        this.dataMode = dataMode;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getResponseType() {
        return responseType;
    }

    public void setResponseType(String responseType) {
        this.responseType = responseType;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getParameters() {
        return parameters;
    }

    public void setParameters(String parameters) {
        this.parameters = parameters;
    }

    public String getRequestTimes() {
        return requestTimes;
    }

    public void setRequestTimes(String requestTimes) {
        this.requestTimes = requestTimes;
    }

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
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

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
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

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return configMap
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("apiUrl", apiUrl);
        configMap.put("method", method);
        configMap.put("dataMode", dataMode);
        configMap.put("dataPath", dataPath);
        configMap.put("responseType", responseType);
        configMap.put("header", header);
        configMap.put("parameters", parameters);
        configMap.put("requestTimes", requestTimes);
        configMap.put("authType", authType);
        configMap.put("username", username);
        configMap.put("password", password);
        configMap.put("token", token);
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
                "\"apiUrl\":\"" + apiUrl + '\"' +
                ", \"method\":\"" + method + '\"' +
                ", \"dataMode\":\"" + dataMode + '\"' +
                ", \"dataPath\":\"" + dataPath + '\"' +
                ", \"responseType\":\"" + responseType + '\"' +
                ", \"header\":\"" + header + '\"' +
                ", \"parameters\":\"" + parameters + '\"' +
                ", \"requestTimes\":\"" + requestTimes + '\"' +
                ", \"authType\":\"" + authType + '\"' +
                ", \"username\":\"" + username + '\"' +
                ", \"password\":\"" + password + '\"' +
                ", \"token\":\"" + token + '\"' +
                ", \"sourceColumns\":\"" + sourceColumns + '\"' +
                ", \"offsetColumns\":\"" + offsetColumns + '\"' +
                '}';
    }
}
