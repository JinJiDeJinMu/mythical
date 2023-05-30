package com.jm.dispatch.python.controller;

import cn.hutool.json.JSONUtil;
import com.jm.dispatch.python.dispatch.PythonDispatch;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.HashMap;
import java.util.Map;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/30 17:41
 */
@RequestMapping("/dispatch/python")
public class PythonController {

    Map<String,PythonDispatch> dispatchMap = new HashMap<>();

    @PostMapping("/run")
    public void run(String parameters){
        PythonDispatch dispatch = new PythonDispatch(parameters);
        String uid = JSONUtil.parseObj(parameters).getStr("uid");
        dispatchMap.put(uid,dispatch);

        dispatch.run();
    }

    @PostMapping(value = "cancel")
    public void cancel(String uid){
        if(dispatchMap.containsKey(uid)){
            dispatchMap.get(uid).cancel();
        }
    }
}
