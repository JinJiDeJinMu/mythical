package com.jm.mythical.k8s.model.req;

import lombok.Data;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/27 15:07
 */
@Data
public class GetDeploymentReq {

    private String namespace;
    private String deploymentName;
}
