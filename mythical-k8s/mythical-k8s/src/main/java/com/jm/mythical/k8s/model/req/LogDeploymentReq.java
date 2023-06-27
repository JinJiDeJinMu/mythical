package com.jm.mythical.k8s.model.req;

import lombok.Data;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/27 15:11
 */
@Data
public class LogDeploymentReq {
    private String namespace;
    private String deploymentName;
}
