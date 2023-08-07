package com.jm.mythical.k8s.operator.spark.deployment;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/3 16:49
 */
public class KubernetesClientAdapter {

    KubernetesClient kubernetesClient;

    public KubernetesClientAdapter(String k8sMasterUrl, String k8sCarCertData, String k8sClientCrtData, String K8sClientKeyData) {
        io.fabric8.kubernetes.client.Config config = new ConfigBuilder()
                .withMasterUrl(k8sMasterUrl)
                .withCaCertData(k8sCarCertData)
                .withClientCertData(k8sClientCrtData)
                .withClientKeyData(K8sClientKeyData)
                .build();


        this.kubernetesClient = new KubernetesClientBuilder()
                .withConfig(config)
                .build();
    }

    public KubernetesClient getKubernetesClient() {
        return kubernetesClient;
    }

    public void closeKubernetesClient() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }
}
