package com.jm.datasource.core.plugin;

import com.jm.model.req.DatasourceBaseReq;
import com.jm.model.req.UnifyTableReq;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/7 14:25
 */
public abstract class AbstractPlugin implements Plugin {

    //todo 是否可以考虑缓存一下datasource
    DataSource dataSource;

    protected abstract String getDriverClassName();

    @Override
    public Connection getConn(DatasourceBaseReq datasourceBaseReq) {
        try {
            if (dataSource == null) {
                this.dataSource = getDataSource(datasourceBaseReq);
            }
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract DataSource getDataSource(DatasourceBaseReq datasourceBaseReq);

    /**
     * 构建查询表的sql
     *
     * @return
     */
    protected abstract String buildTableSQLBySchema(UnifyTableReq unifyTableReq);

}
