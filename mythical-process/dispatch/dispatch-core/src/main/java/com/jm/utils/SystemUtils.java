/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jm.utils;


import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;


public class SystemUtils {

    private SystemUtils() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * get sudo command
     *
     * @param tenantCode tenantCode
     * @param command    command
     * @return result of sudo execute command
     */
    public static String getSudoCmd(String tenantCode, String command) {
        return StringUtils.isEmpty(tenantCode) ? command : "sudo -u " + tenantCode + " " + command;
    }

    /**
     * whether is macOS
     *
     * @return true if mac
     */
    public static boolean isMacOS() {
        return getOSName().startsWith("Mac");
    }

    /**
     * whether is windows
     *
     * @return true if windows
     */
    public static boolean isWindows() {
        return getOSName().startsWith("Windows");
    }


    public static String getOSName() {
        return System.getProperty("os.name");
    }


    public static String[] buildKillCmd(int processId, boolean force) {
        String pid = String.valueOf(processId);
        if (SystemUtils.isWindows()) {
            return force ? new String[]{"taskkill", "-f", "-pid", pid} :
                    new String[]{"taskkill", "-pid", pid};
        } else {
            String signum = force ? "-9" : "-15";
            return new String[]{"kill", signum, pid};
        }
    }

    public static int getProcessId(Process process) {
        try {
            String fieldName = SystemUtils.isWindows() ? "handle" : "pid";
            Field f = process.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);

            int processId = (int) f.getLong(process);
            return processId;
        } catch (Exception e) {
            throw new RuntimeException("获取进程ID异常", e);
        }
    }

    public static List<String> buildCmdArgs() {
        List<String> args = new ArrayList<>();
        if (SystemUtils.isWindows()) {
            args.add("cmd.exe");
            args.add("/c");
        } else {
            args.add("bash");
            args.add("-c");
        }
        return args;
    }
}
