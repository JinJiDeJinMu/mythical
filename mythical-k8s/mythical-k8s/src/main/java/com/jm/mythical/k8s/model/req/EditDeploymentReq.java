package com.jm.mythical.k8s.model.req;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import lombok.Data;

import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/27 15:21
 */
@Data
public class EditDeploymentReq {

    private String namespace;
    private String deploymentName;
    private ObjectMeta metadata;
    private DeploymentSpec spec;
    private DeploymentStatus status;
    private Map<String, Object> additionalProperties;
}
