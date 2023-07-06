package com.jm.mythical.k8s.service.impl;


import cn.hutool.core.map.MapUtil;
import com.jm.mythical.k8s.config.K8sClientConfig;
import com.jm.mythical.k8s.service.IK8sPodService;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/20 16:12
 */
@Service
public class IK8sPodServiceImpl implements IK8sPodService {

    @Resource
    K8sClientConfig k8sClientConfig;

    @Override
    public PodList list(String namespace, Map<String, String> labels) {
        try {
            NonNamespaceOperation<Pod, PodList, PodResource> operation = k8sClientConfig.getClient().pods().inNamespace(namespace);
            if (MapUtil.isNotEmpty(labels)) {
                operation.withLabels(labels);
            }
            return operation.list();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Pod create(String namespace, PodBuilder podBuilder) {
        try {
            return k8sClientConfig.getClient().pods().inNamespace(namespace).resource(podBuilder.build()).create();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Pod create(String namespace, String podName, String path) {
        return null;
    }

    @Override
    public Pod get(String namespace, String podName) {
        try {
            return k8sClientConfig.getClient().pods().inNamespace(namespace).withName(podName).get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Pod edit(String namespace, String podName, PodBuilder podBuilder) {
        try {
            return k8sClientConfig.getClient().pods().inNamespace(namespace).withName(podName).edit(e -> podBuilder.build());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public List<StatusDetails> delete(String namespace, String podName) {
        try {
            return k8sClientConfig.getClient().pods().inNamespace(namespace).withName(podName).delete();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String log(String namespace, String podName) {
        try {
            //todo 判断一下pod的状态再获取日志
            return k8sClientConfig.getClient().pods().inNamespace(namespace).withName(podName).getLog();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public LogWatch logWatch(String namespace, String podName, Integer tailLine, OutputStream outputStream) {
        try {
            return k8sClientConfig.getClient().pods().inNamespace(namespace).withName(podName).tailingLines(tailLine).watchLog(outputStream);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
