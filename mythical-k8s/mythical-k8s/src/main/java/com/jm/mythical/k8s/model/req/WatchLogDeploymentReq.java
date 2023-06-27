package com.jm.mythical.k8s.model.req;

import lombok.Data;

import java.io.OutputStream;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/27 15:13
 */
@Data
public class WatchLogDeploymentReq {

    private String namespace;
    private String deploymentName;
    private Integer tailLine;
    private OutputStream outputStream;
}
