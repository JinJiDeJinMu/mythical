package com.jm.datasource.core.config;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/7 15:44
 */
public class DruidPoolConfig {

    public static final long VALIDATION_QUERY_TIMEOUT = 5000L;

    public static final long TIMEBETWEENEVICTIONRUNSMILLIS = 600000;

    public static final boolean REMOVEABANDONED = true;

    public static final long REMOVEABANDONEDTIMEOUT = 12 * 60 * 60;

    public static final int initialSize = 0;

    public static final int maxActive = 30;

    public static final int minIdle = 1;

    public static final int maxIdle = 8;

    public static final long maxWait = 5000L;

    public final static String validationQuery = "select 1";

    public final static int SOCKET_TIMEOUT_INSECOND = 172800;

}
