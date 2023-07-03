package com.jm.dispatch.hive.dispatch;

import com.jm.dispatch.AbstractSqlDispatch;
import com.jm.param.SqlDispatchParameters;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.regex.Matcher;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/3 14:33
 */
public class HiveDispatch extends AbstractSqlDispatch<SqlDispatchParameters> {

    public HiveDispatch(String dispatchContext) {
        super(dispatchContext);
    }

    @Override
    protected void handleRunLog(Integer index, String sql, Statement statement) {
        if (statement instanceof HiveStatement) {
            HiveStatement hs = (HiveStatement) statement;
            FutureTask<String> future = new FutureTask<>(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return printHiveLogs(hs);
                }
            });
            Thread logThread = new Thread(future, "hive-log-" + dispatchContext.getUid());
            logThread.setDaemon(true);
            logThread.start();
        }
    }

    private String printHiveLogs(HiveStatement hs) {
        String applicationId = "";
        try {
            while (hs.hasMoreLogs()) {
                List<String> queryLog = hs.getQueryLog();
                if (CollectionUtils.isEmpty(queryLog)) {
                    Thread.sleep(200L);
                } else {
                    for (String s : queryLog) {
                        LOG.info(s);
                        Matcher matcher = APPLICATION_REGEX.matcher(s);
                        if (matcher.find()) {
                            applicationId = matcher.group();
                        }

                    }

                }
            }
            // 二次读取
            int count = 0;
            while (count++ < 100) {
                List<String> queryLog = hs.getQueryLog();
                if (CollectionUtils.isEmpty(queryLog)) {
                    break;
                } else {
                    for (String s : queryLog) {
                        LOG.info(s);
                        Matcher matcher = APPLICATION_REGEX.matcher(s);
                        if (matcher.find()) {
                            applicationId = matcher.group();
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return applicationId;
    }
}
