package com.jm.mythical.k8s.operator.spark.deployment;

import com.jm.mythical.k8s.operator.spark.deployment.context.Context;
import com.jm.mythical.k8s.operator.spark.deployment.context.SparkConfig;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/3 16:38
 */
public abstract class AbstractKubernetesClusterDeployment<P extends Context> implements ClusterDeployment {

    protected SparkConfig sparkConfig;


    public AbstractKubernetesClusterDeployment(P p) {

    }

    public AbstractKubernetesClusterDeployment(String s) {

    }

    public abstract void preSubmit();

    public abstract void doSubmit();

    protected abstract void postSubmit();


    @Override
    public void process() {

        try {
            preSubmit();

            doSubmit();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            postSubmit();
        }
    }

}
