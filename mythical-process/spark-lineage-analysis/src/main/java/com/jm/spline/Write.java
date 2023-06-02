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
public class Write {

    private String outputSource;
    private boolean append;
    private String id;
    private String name;
    private List<String> childIds;
    private Extra extra;
    private List<String> output;
    private Map<String, String> params;
}