package com.jm.spline;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/4 11:31
 */
@Data
public class Attributes {

    private String id;
    private String dataType;
    private String name;
    private List<Map<String, String>> childRefs;

}