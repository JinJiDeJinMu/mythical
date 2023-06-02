package com.jm.spline;

import lombok.Data;

import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/8 17:13
 */
@Data
public class SplineInfo {

    private SplineNodeInfo writeNodeInfo;

    private List<SplineNodeInfo> readNodeInfo;

    private List<SplineEdgeInfo> splineEdgeInfo;

    private List<Attributes> attributes;

}
