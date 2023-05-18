package com.jm.dispatch;

import com.jm.helper.SQLHelper;
import com.jm.param.SqlDispatchParameters;

import java.io.InputStream;
import java.sql.*;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 20:44
 */
public abstract class AbstractSqlDispatch <P extends SqlDispatchParameters> extends AbstractDispatch<P>{

    private Connection connection;

    private Statement statement;

    private static final int maxSQLNum =  10000;

    public AbstractSqlDispatch(String dispatchContext) {
        super(dispatchContext);
    }

    @Override
    protected Class getParametersClass() {
        return SqlDispatchParameters.class;
    }

    @Override
    protected void preRun() {
        this.connection = connect();

    }


    @Override
    protected void doRun() {
        try {
            if(connection == null || connection.isClosed()){
                this.connection = connect();
            }

            executeSql();
            
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    protected void executeSql() {
        SQLHelper sqlHelper = new SQLHelper();

        List<String> analyzeSQL = sqlHelper.analyzeSQL(dispatchContext.getDispatchType(), parameters.getCode());

        for (int i = 0; i < analyzeSQL.size(); i++) {
            String sql = analyzeSQL.get(i);
            try {
                statement = connect().createStatement();
                statement.setMaxRows(maxSQLNum);
                statement.setFetchSize(maxSQLNum);

                if(sqlHelper.checkInsert(sql)){
                    statement.execute(sql);
                } else if (sqlHelper.checkUpdate(sql)) {
                    statement.executeUpdate(sql);
                } else if (sqlHelper.checkSelect(sql)) {
                    ResultSet resultSet = statement.executeQuery(sql);
                    //todo 结果处理
                    InputStream csvStream = sqlHelper.transformCSVStream(resultSet);
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void postRun() {

    }

    protected Connection connect() {
        try {
            String url = parameters.getUrl().contains(parameters.getDatabase()) ? parameters.getUrl(): parameters.getUrl() +"/" +parameters.getDatabase();

            Properties properties = new Properties();

            properties.setProperty("url", url);
            properties.setProperty("user", parameters.getUsername());
            properties.setProperty("password", parameters.getPassword());

            Optional<String> optional = Optional.ofNullable(parameters.getDriverClassName());
            if(optional.isPresent()){
                properties.setProperty("driveClassName", optional.get());
            }
            return DriverManager.getConnection(url,properties);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
