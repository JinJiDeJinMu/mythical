package com.jm.datasource.core.pool;

import cn.hutool.core.map.MapUtil;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.jm.datasource.core.config.DruidPoolConfig;
import com.jm.model.req.DatasourceBaseReq;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/7 15:32
 */
public class DruidDatasourcePoolManager {


    public static DataSource buildDataSource(String driverClassName, DatasourceBaseReq datasourceBaseReq) {
        Map<String, Object> properties = new HashMap<>();

        properties.put(DruidDataSourceFactory.PROP_DRIVERCLASSNAME, driverClassName);
        properties.put(DruidDataSourceFactory.PROP_URL, datasourceBaseReq.getUrl());
        properties.put(DruidDataSourceFactory.PROP_USERNAME, datasourceBaseReq.getUsername());
        properties.put(DruidDataSourceFactory.PROP_PASSWORD, datasourceBaseReq.getPassword());

        properties.put(DruidDataSourceFactory.PROP_MAXACTIVE, DruidPoolConfig.maxActive);
        properties.put(DruidDataSourceFactory.PROP_MAXWAIT, DruidPoolConfig.maxWait);
        properties.put(DruidDataSourceFactory.PROP_MINIDLE, DruidPoolConfig.minIdle);
        properties.put(DruidDataSourceFactory.PROP_TIMEBETWEENEVICTIONRUNSMILLIS, DruidPoolConfig.TIMEBETWEENEVICTIONRUNSMILLIS);
        properties.put(DruidDataSourceFactory.PROP_REMOVEABANDONED, DruidPoolConfig.REMOVEABANDONED);
        properties.put(DruidDataSourceFactory.PROP_REMOVEABANDONEDTIMEOUT, DruidPoolConfig.REMOVEABANDONEDTIMEOUT);

        if (MapUtil.isNotEmpty(datasourceBaseReq.getExtraParams())) {
            properties.putAll(datasourceBaseReq.getExtraParams());
        }

        //todo auth 校验

        try {
            return DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
