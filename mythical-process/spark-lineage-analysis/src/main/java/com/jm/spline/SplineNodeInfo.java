package com.jm.spline;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/8 17:28
 */
@Data
public class SplineNodeInfo {

    private String name;
    private String sourceType;
    private List<String> output;
    private Map<String, String> params;

    public SplineNodeInfo(String name, String sourceType, List<String> output, Map<String, String> params) {
        this.name = name;
        this.sourceType = sourceType;
        this.output = output;
        this.params = params;
    }
}
