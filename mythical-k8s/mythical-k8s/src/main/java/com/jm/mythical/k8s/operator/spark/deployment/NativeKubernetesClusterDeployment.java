package com.jm.mythical.k8s.operator.spark.deployment;

import com.jm.mythical.k8s.operator.spark.deployment.context.SparkContext;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/23 14:44
 */
public class NativeKubernetesClusterDeployment extends AbstractKubernetesClusterDeployment<SparkContext> {

    public NativeKubernetesClusterDeployment(SparkContext sparkContext) {
        super(sparkContext);
    }

    @Override
    public void preSubmit() {

    }

    @Override
    public void doSubmit() {

    }

    @Override
    protected void postSubmit() {

    }

    @Override
    public void init() {

    }

    @Override
    public void cancel() {

    }
}
