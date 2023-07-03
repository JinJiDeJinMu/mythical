package com.jm.dispatch.hive.controller;

import cn.hutool.json.JSONUtil;
import com.jm.dispatch.hive.dispatch.HiveDispatch;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/3 16:12
 */
@RestController
@RequestMapping(value = "/dispatch/hive")
public class HiveController {

    Map<String, HiveDispatch> map = new HashMap<>();

    @PostMapping(value = "run")
    public void run(String parameters) {
        HiveDispatch hiveDispatch = new HiveDispatch(parameters);
        String uid = JSONUtil.parseObj(parameters).getStr("uid");

        map.put(uid, hiveDispatch);

        hiveDispatch.run();
    }

    @PostMapping(value = "/cancel")
    public void cancel(String uid) {
        map.get(uid).cancel();
    }
}
