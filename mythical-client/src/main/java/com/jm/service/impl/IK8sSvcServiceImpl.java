package com.jm.service.impl;

import com.jm.config.K8sClientConfig;
import com.jm.service.Ik8sSvcService;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.StatusDetails;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Resource;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/21 11:04
 */
@org.springframework.stereotype.Service
public class IK8sSvcServiceImpl implements Ik8sSvcService {

    @Resource
    K8sClientConfig k8sClientConfig;

    @Override
    public Service get(String namespace, String SvcName) {
        try {
            return k8sClientConfig.getClient().services().inNamespace(namespace).withName(SvcName).get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public ServiceList list(String namespace) {
        try {
            return StringUtils.isNotBlank(namespace) ?
                    k8sClientConfig.getClient().services().inNamespace(namespace).list() :
                    k8sClientConfig.getClient().services().inAnyNamespace().list();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Service create(String namespace, String SvcName, ServiceBuilder serviceBuilder) {
        try {
            return k8sClientConfig.getClient().services().inNamespace(namespace).resource(serviceBuilder.build()).create();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public List<StatusDetails> delete(String namespace, String SvcName) {
        try {
            return k8sClientConfig.getClient().services().inNamespace(namespace).withName(SvcName).delete();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Service edit(String namespace, String ScvName, ServiceBuilder serviceBuilder) {
        try {
            return k8sClientConfig.getClient().services().inNamespace(namespace).withName(ScvName).edit(e -> serviceBuilder.build());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
