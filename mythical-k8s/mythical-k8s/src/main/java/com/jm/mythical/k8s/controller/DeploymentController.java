package com.jm.mythical.k8s.controller;

import com.jm.mythical.k8s.facade.DeploymentFacade;
import com.jm.mythical.k8s.model.req.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.InputStream;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/27 14:06
 */
@RestController
@RequestMapping(value = "/deployment")
public class DeploymentController {

    @Autowired
    DeploymentFacade deploymentFacade;

    @PostMapping(value = "/create")
    public Boolean create(@RequestBody CreateDeploymentReq createDeploymentReq) {
        return deploymentFacade.createDeployment(createDeploymentReq);
    }

    @PostMapping(value = "/operator")
    public Boolean operator(@RequestBody OperatorDeploymentReq operatorDeploymentReq) {
        return deploymentFacade.operatorDeployment(operatorDeploymentReq);
    }

    @GetMapping(value = "/list")
    public DeploymentList list(String namespace) {
        return deploymentFacade.listDeployment(namespace);
    }

    @PostMapping(value = "/edit")
    public Boolean edit(@RequestBody EditDeploymentReq editDeploymentReq) {
        return deploymentFacade.editDeployment(editDeploymentReq);
    }

    @PostMapping(value = "/log")
    public String log(@RequestBody LogDeploymentReq logDeploymentReq) {
        return deploymentFacade.logDeployment(logDeploymentReq);
    }

    @PostMapping(value = "/watchLog")
    public InputStream watchLog(@RequestBody WatchLogDeploymentReq watchLogDeploymentReq) {
        return deploymentFacade.watchLogDeployment(watchLogDeploymentReq);
    }

    @PostMapping(value = "/get")
    public Deployment get(GetDeploymentReq getDeploymentReq) {
        return deploymentFacade.getDeployment(getDeploymentReq);
    }

}
