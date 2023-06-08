package com.jm.datasource.core.plugin;

import com.jm.model.req.DatasourceBaseReq;
import com.jm.model.req.UnifyDatasourceReq;
import com.jm.model.req.UnifyTableReq;
import com.jm.model.res.ColumnInfoRes;
import com.jm.model.res.DatabaseInfoRes;
import com.jm.model.res.TableInfoRes;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/7 10:15
 */
public interface Plugin {

    Connection getConn(DatasourceBaseReq datasourceBaseReq);

    Boolean testConn(DatasourceBaseReq datasourceBaseReq);

    List<String> databaseList(UnifyDatasourceReq unifyDatasourceReq);

    List<String> tableList(DatasourceBaseReq datasourceBaseReq, UnifyTableReq unifyTableReq);

    List<String> tableListBySQL(DatasourceBaseReq datasourceBaseReq, UnifyTableReq unifyTableReq);

    List<ColumnInfoRes> columnList(UnifyDatasourceReq unifyDatasourceReq);

    DatabaseInfoRes databaseDetail();

    TableInfoRes tableDetail();

    Boolean executeSQL(DatasourceBaseReq datasourceBaseReq, String sql);

    List<Map<String, Object>> querySQL(DatasourceBaseReq datasourceBaseReq, String sql);

}
