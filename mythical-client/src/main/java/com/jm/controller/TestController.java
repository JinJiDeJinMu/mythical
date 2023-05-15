package com.jm.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 13:41
 */
@RestController
@RequestMapping("/test")
public class TestController {

    @GetMapping("/heart")
    public String checkHeart(){
        return "心跳检测成功";
    }
}
