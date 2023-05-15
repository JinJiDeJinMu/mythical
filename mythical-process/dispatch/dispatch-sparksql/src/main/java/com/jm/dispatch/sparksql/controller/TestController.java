package com.jm.dispatch.sparksql.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 15:33
 */
@RestController
@RequestMapping("/dispatch")
public class TestController {

    @GetMapping(value = "/sparksql")
    public String test(){
        return "spark sql test";
    }
}
