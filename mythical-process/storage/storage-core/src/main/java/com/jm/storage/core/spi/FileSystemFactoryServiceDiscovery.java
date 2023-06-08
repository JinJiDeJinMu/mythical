package com.jm.storage.core.spi;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * TODO spi加载client
 *
 * @Author jinmu
 * @Date 2023/3/22 19:09
 */
public class FileSystemFactoryServiceDiscovery {

    private static volatile boolean load = false;

    private static Map<String, FileSystemClientFactory<FileSystemClient, Object>> fsClientFactoryMap;

    public static FileSystemClientFactory<FileSystemClient, Object> get(String clientType) {
        load();
        return fsClientFactoryMap.get(clientType);
    }

    public static void load(){
        if (load) {
            return;
        }
        synchronized (FileSystemFactoryServiceDiscovery.class) {
            if (load) {
                return;
            }

            fsClientFactoryMap = new HashMap<>();

            ServiceLoader<FileSystemClientFactory> factoryLoader = ServiceLoader.load(FileSystemClientFactory.class);
            Iterator<FileSystemClientFactory> it = factoryLoader.iterator();

            while (it.hasNext()) {
                FileSystemClientFactory clientFactory = it.next();
                String supportType = clientFactory.supportType();

                if (fsClientFactoryMap.containsKey(supportType)) {
                    FileSystemClientFactory existsFactory = fsClientFactoryMap.get(supportType);
                    throw new IllegalArgumentException(String.format("('%s') 同时被'%s','%s'支持！)",
                            supportType, existsFactory.getClass().getName(), clientFactory.getClass().getName()));
                }
                fsClientFactoryMap.put(supportType, clientFactory);
            }
            load = true;
        }

    }

}
