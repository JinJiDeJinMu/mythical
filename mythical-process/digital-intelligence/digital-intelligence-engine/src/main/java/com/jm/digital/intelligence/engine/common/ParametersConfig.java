package com.jm.digital.intelligence.engine.common;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/5 16:52
 */
public class ParametersConfig {

    public static Map<String,String> transformMap(HttpServletRequest request){
        Map<String,String> map = new HashMap<>();
        request.getParameterMap().forEach((k,v)->map.put(k,v[0]));
        return map;
    }
}
