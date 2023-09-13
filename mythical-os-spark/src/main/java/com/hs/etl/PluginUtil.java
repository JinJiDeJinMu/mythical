package com.hs.etl;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.hs.etl.sink.*;
import com.hs.etl.source.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class PluginUtil {

    private static final Map<String, Class<?>> SOURCE_PLUGINS = getSourcePlugins();
    private static final Map<String, Class<?>> SINK_PLUGINS = getSinkPlugins();

    private static Map<String, Class<?>> getSourcePlugins() {
        Map<String, Class<?>> classMap = new HashMap<>();
        classMap.put("jdbc", JdbcSource.class);
        classMap.put("hive", HiveSource.class);
        //classMap.put("es", EsSource.class);
        classMap.put("mongo", MongoSource.class);
        classMap.put("redis", RedisSource.class);
        classMap.put("api", ApiSource.class);
        classMap.put("csv", CsvSource.class);
        classMap.put("excel", ExcelSource.class);
        classMap.put("ftp", FtpSource.class);
        classMap.put("delta", DeltaSource.class);
        return classMap;
    }


    private static Map<String, Class<?>> getSinkPlugins() {
        Map<String, Class<?>> classMap = new HashMap<>();
        classMap.put("jdbc", JdbcSink.class);
        classMap.put("hive", HiveSink.class);
        //classMap.put("es", EsSink.class);
        classMap.put("mongo", MongoSink.class);
        classMap.put("delta", DeltaSink.class);
        classMap.put("starrocks", StarrocksSink.class);
        return classMap;
    }

    public static <T> EtlPlugin<T> createSource(
            String type, JsonElement config)
            throws InstantiationException, IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        return createPlugin(SOURCE_PLUGINS, type, config);
    }


    public static <T> EtlPlugin<T> createSink(String type, JsonElement config)
            throws InstantiationException, IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        return createPlugin(SINK_PLUGINS, type, config);
    }

    static <T extends EtlPlugin> T createPlugin(
            Map<String, Class<?>> pluginMap, String name, JsonElement config)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException,
            InvocationTargetException {
        Class<?> type = pluginMap.get(name);
        ParameterizedType genericSuperclass = (ParameterizedType) type.getGenericInterfaces()[0];
        Class<?> configType = (Class<?>) genericSuperclass.getActualTypeArguments()[0];
        T plugin = (T) type.getDeclaredConstructor().newInstance();
        plugin.setConfig(new GsonBuilder().create().fromJson(config, configType));
        return plugin;
    }
}
