package com.jm.mythical.k8s.facade;

import cn.hutool.core.map.MapUtil;
import com.jm.mythical.k8s.model.req.*;
import com.jm.mythical.k8s.service.IK8sDeploymentService;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/27 14:09
 */
@Service
public class DeploymentFacade {

    @Autowired
    IK8sDeploymentService ik8sDeploymentService;


    public DeploymentList listDeployment(String namespace) {
        return ik8sDeploymentService.list(namespace);
    }


    public boolean createDeployment(CreateDeploymentReq createDeploymentReq) {
        DeploymentBuilder deploymentBuilder = new DeploymentBuilder().withKind(createDeploymentReq.getKind()).withApiVersion(createDeploymentReq.getApiVersion());
        if (createDeploymentReq.getMetadata() != null) {
            deploymentBuilder.withMetadata(createDeploymentReq.getMetadata());
        }
        if (createDeploymentReq.getSpec() != null) {
            deploymentBuilder.withSpec(createDeploymentReq.getSpec());
        }
        if (createDeploymentReq.getStatus() != null) {
            deploymentBuilder.withStatus(createDeploymentReq.getStatus());
        }
        if (MapUtil.isNotEmpty(createDeploymentReq.getAdditionalProperties())) {
            deploymentBuilder.withAdditionalProperties(createDeploymentReq.getAdditionalProperties());
        }

        Deployment deployment = ik8sDeploymentService.create(createDeploymentReq.getNamespace(), deploymentBuilder);

        //todo 可以做一些其他操作
        return deployment != null && createDeploymentReq.getApiVersion().equalsIgnoreCase(deployment.getApiVersion());
    }

    public Boolean operatorDeployment(OperatorDeploymentReq operatorDeploymentReq) {
        Boolean flag = Boolean.TRUE;
        switch (operatorDeploymentReq.getOperatorType()) {
            case "delete":
                List<StatusDetails> statusDetails = ik8sDeploymentService.delete(operatorDeploymentReq.getNamespace(), operatorDeploymentReq.getDeploymentName());
                break;
            case "undo":
                Deployment undo = ik8sDeploymentService.undo(operatorDeploymentReq.getNamespace(), operatorDeploymentReq.getDeploymentName());
                break;
            case "pause":
                Deployment pause = ik8sDeploymentService.pause(operatorDeploymentReq.getNamespace(), operatorDeploymentReq.getDeploymentName());
                break;
            case "resume":
                Deployment resume = ik8sDeploymentService.resume(operatorDeploymentReq.getNamespace(), operatorDeploymentReq.getDeploymentName());
                break;
            case "restart":
                Deployment restart = ik8sDeploymentService.restart(operatorDeploymentReq.getNamespace(), operatorDeploymentReq.getDeploymentName());
                break;
            default:
                throw new RuntimeException("不支持的operatorType = " + operatorDeploymentReq.getOperatorType());
        }
        return flag;
    }

    public Deployment getDeployment(GetDeploymentReq getDeploymentReq) {
        return ik8sDeploymentService.get(getDeploymentReq.getNamespace(), getDeploymentReq.getDeploymentName());
    }

    public String logDeployment(LogDeploymentReq logDeploymentReq) {
        return ik8sDeploymentService.log(logDeploymentReq.getNamespace(), logDeploymentReq.getDeploymentName());
    }

    public InputStream watchLogDeployment(WatchLogDeploymentReq watchLogDeploymentReq) {
        LogWatch logwatch = ik8sDeploymentService.logwatch(watchLogDeploymentReq.getNamespace(), watchLogDeploymentReq.getDeploymentName(), watchLogDeploymentReq.getTailLine(), watchLogDeploymentReq.getOutputStream());
        if (logwatch != null) {
            return logwatch.getOutput();
        }
        return null;
    }

    public Boolean editDeployment(EditDeploymentReq editDeploymentReq) {
        DeploymentBuilder deploymentBuilder = new DeploymentBuilder();

        if (editDeploymentReq.getMetadata() != null) {
            deploymentBuilder.withMetadata(editDeploymentReq.getMetadata());
        }
        if (editDeploymentReq.getSpec() != null) {
            deploymentBuilder.withSpec(editDeploymentReq.getSpec());
        }
        if (editDeploymentReq.getStatus() != null) {
            deploymentBuilder.withStatus(editDeploymentReq.getStatus());
        }
        if (MapUtil.isNotEmpty(editDeploymentReq.getAdditionalProperties())) {
            deploymentBuilder.withAdditionalProperties(editDeploymentReq.getAdditionalProperties());
        }
        Deployment deployment = ik8sDeploymentService.edit(editDeploymentReq.getNamespace(), editDeploymentReq.getDeploymentName(), deploymentBuilder);

        return deployment != null;
    }

}
