package com.jm.mythical.k8s.service;

import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.dsl.LogWatch;

import java.io.OutputStream;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/21 13:59
 */
public interface IK8sDeploymentService {

    Deployment get(String namespace, String deploymentName);

    Deployment create(String namespace, DeploymentBuilder deploymentBuilder);

    Deployment edit(String namespace, String deploymentName, DeploymentBuilder deploymentBuilder);

    Deployment createOrReplace(String namespace, DeploymentBuilder deploymentBuilder);

    DeploymentList list(String namespace);

    Deployment restart(String namespace, String deploymentName);

    Deployment pause(String namespace, String deploymentName);

    Deployment resume(String namespace, String deploymentName);

    Deployment undo(String namespace, String deploymentName);

    List<StatusDetails> delete(String namespace, String deploymentName);

    String log(String namespace, String deploymentName);

    LogWatch logwatch(String namespace, String deploymentName, Integer tailLines, OutputStream outputStream);

}
