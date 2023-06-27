package com.jm.mythical.k8s.model.req;

import lombok.Data;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/27 14:39
 */
@Data
public class OperatorDeploymentReq {

    private String namespace;
    private String deploymentName;
    /**
     * delete
     * pause
     * restart
     * resume
     * undo
     */
    private String operatorType;
}
