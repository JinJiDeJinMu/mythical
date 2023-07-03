package com.jm.mythical.k8s.flink;

import com.jm.mythical.k8s.config.K8sClientConfig;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.InputStream;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/27 21:04
 */
@Service
public class FlinkK8sOperator {

    @Resource
    private K8sClientConfig k8sClientConfig;

    public FlinkDeployment create(FlinkDeployment flinkDeployment) {
        return k8sClientConfig.getClient().resource(flinkDeployment).create();
    }

    public HasMetadata create(InputStream inputStream) {
        return k8sClientConfig.getClient().resource(inputStream).create();
    }

    public NamespaceableResource<HasMetadata> create(String resource) {
        return k8sClientConfig.getClient().resource(resource);
    }
}
