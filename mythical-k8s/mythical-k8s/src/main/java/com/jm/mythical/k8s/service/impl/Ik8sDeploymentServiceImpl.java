package com.jm.mythical.k8s.service.impl;


import com.jm.mythical.k8s.config.K8sClientConfig;
import com.jm.mythical.k8s.service.IK8sDeploymentService;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.OutputStream;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/21 14:53
 */
@Service
public class Ik8sDeploymentServiceImpl implements IK8sDeploymentService {

    @Resource
    K8sClientConfig k8sClientConfig;

    @Override
    public Deployment get(String namespace, String deploymentName) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Deployment create(String namespace, DeploymentBuilder deploymentBuilder) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).resource(deploymentBuilder.build()).create();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Deployment edit(String namespace, String deploymentName, DeploymentBuilder deploymentBuilder) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).edit(e -> deploymentBuilder.build());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Deployment createOrReplace(String namespace, DeploymentBuilder deploymentBuilder) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).createOrReplace(deploymentBuilder.build());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public DeploymentList list(String namespace) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).list();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Deployment restart(String namespace, String deploymentName) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).rolling().restart();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Deployment pause(String namespace, String deploymentName) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).rolling().pause();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Deployment resume(String namespace, String deploymentName) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).rolling().resume();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Deployment undo(String namespace, String deploymentName) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).rolling().undo();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public List<StatusDetails> delete(String namespace, String deploymentName) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).delete();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String log(String namespace, String deploymentName) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).getLog();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public LogWatch logwatch(String namespace, String deploymentName, Integer tailLines, OutputStream outputStream) {
        try {
            return k8sClientConfig.getClient().apps().deployments().inNamespace(namespace).withName(deploymentName).tailingLines(tailLines == null ? 0 : tailLines).watchLog(outputStream);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
