package com.jm.mythical.k8s.operator.spark.deployment;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/3 13:58
 */
public interface ClusterDeployment {

     void init();

     void process();

     void cancel();
}
