package com.jm.datasource.core.plugin;

import com.jm.datasource.core.pool.DruidDatasourcePoolManager;
import com.jm.datasource.core.utils.DBUtils;
import com.jm.model.req.DatasourceBaseReq;
import com.jm.model.req.UnifyDatasourceReq;
import com.jm.model.req.UnifyTableReq;
import com.jm.model.res.ColumnInfoRes;
import com.jm.model.res.DatabaseInfoRes;
import com.jm.model.res.TableInfoRes;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/7 15:24
 */
public abstract class RdbmsPlugin extends AbstractPlugin {

    private static final Integer defaultFetchSize = 10000;

    @Override
    protected DataSource getDataSource(DatasourceBaseReq datasourceBaseReq) {
        return DruidDatasourcePoolManager.buildDataSource(getDriverClassName(), datasourceBaseReq);
    }


    @Override
    public Boolean testConn(DatasourceBaseReq datasourceBaseReq) {
        try {
            return getConn(datasourceBaseReq).isClosed();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> databaseList(UnifyDatasourceReq unifyDatasourceReq) {
        throw new RuntimeException("databaseList method not support");
    }

    @Override
    public List<String> tableList(DatasourceBaseReq datasourceBaseReq, UnifyTableReq unifyTableReq) {
        List<String> tableList = new ArrayList<>();
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConn(datasourceBaseReq);
            DatabaseMetaData metaData = conn.getMetaData();
            if (null == unifyTableReq) {
                rs = metaData.getTables(null, null, null, null);
            } else {
                rs = metaData.getTables(null, unifyTableReq.getSchema(),
                        StringUtils.isNotBlank(unifyTableReq.getTableNamePattern()) ? unifyTableReq.getTableNamePattern() :
                                unifyTableReq.getTableName(), null);
            }
            while (rs.next()) {
                tableList.add(rs.getString(3));
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBUtils.closeDB(rs, null, conn);
        }
        return tableList;
    }

    @Override
    public List<String> tableListBySQL(DatasourceBaseReq datasourceBaseReq, UnifyTableReq unifyTableReq) {
        return querySingleBySQL(datasourceBaseReq, buildTableSQLBySchema(unifyTableReq), 1);
    }

    /**
     * 统一sql查询返回单列
     *
     * @param datasourceBaseReq
     * @param sql
     * @param columnIndex
     * @return
     */
    protected List<String> querySingleBySQL(DatasourceBaseReq datasourceBaseReq, String sql, Integer columnIndex) {
        Statement statement = null;
        ResultSet rs = null;
        Connection conn = null;
        List<String> result = new ArrayList<>();
        try {
            conn = getConn(datasourceBaseReq);
            statement = conn.createStatement();
            statement.setFetchSize(defaultFetchSize);
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                result.add(rs.getString(columnIndex == null ? 1 : columnIndex));
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            DBUtils.closeDB(rs, statement, conn);
        }
        return result;
    }


    @Override
    public List<ColumnInfoRes> columnList(UnifyDatasourceReq unifyDatasourceReq) {
        return null;
    }

    @Override
    public DatabaseInfoRes databaseDetail() {
        return null;
    }

    @Override
    public TableInfoRes tableDetail() {
        return null;
    }

    @Override
    public Boolean executeSQL(DatasourceBaseReq datasourceBaseReq, String sql) {
        Statement statement = null;
        Connection conn = null;
        try {
            conn = getConn(datasourceBaseReq);
            statement = conn.createStatement();
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBUtils.closeDB(null, statement, conn);
        }
    }

    @Override
    public List<Map<String, Object>> querySQL(DatasourceBaseReq datasourceBaseReq, String sql) {
        List<Map<String, Object>> result = new ArrayList<>();
        ResultSet rs = null;
        Statement statement = null;
        Connection conn = null;
        try {
            conn = getConn(datasourceBaseReq);
            statement = conn.createStatement();
            statement.setMaxRows(defaultFetchSize);

            if (statement.execute(sql)) {
                rs = statement.getResultSet();
                int columns = rs.getMetaData().getColumnCount();
                List<String> columnName = new ArrayList<>();
                for (int i = 0; i < columns; i++) {
                    columnName.add(rs.getMetaData().getColumnLabel(i + 1));
                }
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 0; i < columns; i++) {
                        row.put(columnName.get(i), rs.getObject(i + 1));
                    }
                    result.add(row);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            DBUtils.closeDB(rs, statement, conn);
        }
        return result;
    }

}
