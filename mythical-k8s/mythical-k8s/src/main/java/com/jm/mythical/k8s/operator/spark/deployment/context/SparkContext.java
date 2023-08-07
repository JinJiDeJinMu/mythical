package com.jm.mythical.k8s.operator.spark.deployment.context;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/4 09:50
 */
public class SparkContext implements Context {

    SparkConfig sparkConfig;

    @Override
    public boolean checkParameters() {
        return false;
    }

    public SparkConfig getSparkConfig() {
        return sparkConfig;
    }

    public void setSparkConfig(SparkConfig sparkConfig) {
        this.sparkConfig = sparkConfig;
    }

}
