package com.nebula.hs.etl.config.sink;


import com.google.gson.Gson;

import java.util.Map;

/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class HiveSinkConfig extends JdbcSinkConfig {
    private String hiveHdfsPath;

    public static void main(String[] args) {
        HiveSinkConfig hiveSinkConfig = new HiveSinkConfig();
        hiveSinkConfig.setHiveHdfsPath("hive2222");

        System.out.println(hiveSinkConfig);

        System.out.println(new Gson().toJson(hiveSinkConfig.getConfigMap()));
    }

    public String getHiveHdfsPath() {
        return hiveHdfsPath;
    }

    public void setHiveHdfsPath(String hiveHdfsPath) {
        this.hiveHdfsPath = hiveHdfsPath;
    }

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return configMap
     */
    @Override
    public Map<String, Object> getConfigMap() {
        Map<String, Object> baseConfigMap = super.getConfigMap();
        baseConfigMap.put("hiveHdfsPath", hiveHdfsPath);
        return baseConfigMap;
    }

    /**
     * 当前 config 新增【属性】时,必须在该方法中同步更改
     *
     * @return JsonString
     */
    @Override
    public String toString() {
        return super.toString().substring(0, super.toString().length() - 1) + "" +
                ", \"mergeKeys\":\"" + hiveHdfsPath + '\"' + '}';
    }
}
