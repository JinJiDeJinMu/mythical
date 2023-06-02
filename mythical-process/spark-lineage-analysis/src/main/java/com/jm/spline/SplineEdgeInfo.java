package com.jm.spline;

import lombok.Data;

import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/13 14:36
 */
@Data
public class SplineEdgeInfo {

    private List<String> source;

    private String target;

    public SplineEdgeInfo(List<String> source, String target) {
        this.source = source;
        this.target = target;
    }

}
