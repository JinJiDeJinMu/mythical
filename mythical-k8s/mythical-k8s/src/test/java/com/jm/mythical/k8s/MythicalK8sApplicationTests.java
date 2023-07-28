package com.jm.mythical.k8s;

import com.jm.mythical.k8s.config.K8sClientConfig;
import com.jm.mythical.k8s.service.IK8sDeploymentService;
import com.jm.mythical.k8s.service.IK8sPodService;
import com.jm.mythical.k8s.spark.operator.crds.RestartPolicy;
import com.jm.mythical.k8s.spark.operator.crds.SparkApplication;
import com.jm.mythical.k8s.spark.operator.crds.SparkApplicationSpec;
import com.jm.mythical.k8s.spark.operator.crds.SparkPodSpec;
import io.fabric8.kubernetes.api.model.*;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class MythicalK8sApplicationTests {

    @Autowired
    IK8sPodService ik8sPodService;

    @Autowired
    IK8sDeploymentService ik8sDeploymentService;


    @javax.annotation.Resource
    K8sClientConfig client;

    @Test
    void contextLoads() {
    }


    @Test
    public void getLog() {
        String log = client.getClient().pods().inNamespace("flink-operator").withName("tomcat-test").getLog();
        System.out.println(log);
    }

    @Test
    public void createPod() {
        ObjectMeta metadata = new ObjectMeta();
        metadata.setNamespace("flink-operator");
        metadata.setName("tomcat-test");

        List<Container> containers = new ArrayList<>();
        PodSpec podSpec = new PodSpec();

        Container container = new Container();
        container.setName("tomcat");
        container.setImage("tomcat");
        containers.add(container);

        podSpec.setContainers(containers);
        podSpec.setServiceAccount("flink-operator");

        PodStatus podStatus = new PodStatus();

        PodBuilder podBuilder = new PodBuilder()
                .withKind("Pod")
                .withMetadata(metadata)
                .withSpec(podSpec)
                .withStatus(podStatus);


        Pod pod = ik8sPodService.create("flink-operator", podBuilder);
    }


    @Test
    public void flinkOperatorTest() {
        FlinkDeployment flinkDeployment = new FlinkDeployment();
        flinkDeployment.setApiVersion("flink.apache.org/v1beta1");
        flinkDeployment.setKind("FlinkDeployment");
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setNamespace("flink-operator");
        objectMeta.setName("basic-example");
        flinkDeployment.setMetadata(objectMeta);
        FlinkDeploymentSpec flinkDeploymentSpec = new FlinkDeploymentSpec();
        flinkDeploymentSpec.setFlinkVersion(FlinkVersion.v1_16);
        flinkDeploymentSpec.setImage("flink:1.16");
        Map<String, String> flinkConfiguration = new HashMap<>();
        flinkConfiguration.put("taskmanager.numberOfTaskSlots", "1");
        flinkDeploymentSpec.setFlinkConfiguration(flinkConfiguration);
        flinkDeployment.setSpec(flinkDeploymentSpec);
        flinkDeploymentSpec.setServiceAccount("flink-operator");
        JobManagerSpec jobManagerSpec = new JobManagerSpec();
        jobManagerSpec.setResource(new Resource(1.0, "1024m", "2G"));
        flinkDeploymentSpec.setJobManager(jobManagerSpec);
        TaskManagerSpec taskManagerSpec = new TaskManagerSpec();
        taskManagerSpec.setResource(new Resource(1.0, "1024m", "2G"));
        flinkDeploymentSpec.setTaskManager(taskManagerSpec);
        flinkDeployment
                .getSpec()
                .setJob(
                        JobSpec.builder()
                                .jarURI(
                                        "local:///opt/flink/examples/streaming/StateMachineExample.jar")
                                .parallelism(1)
                                .upgradeMode(UpgradeMode.STATELESS)
                                .build());


        FlinkDeployment deployment = client.getClient().resource(flinkDeployment).create();

    }


    @Test
    public void deleteFlinkDeployment() {
        FlinkDeployment flinkDeployment = new FlinkDeployment();
        flinkDeployment.setApiVersion("flink.apache.org/v1beta1");
        flinkDeployment.setKind("FlinkDeployment");
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setNamespace("flink-operator");
        objectMeta.setName("basic-example");


        List<StatusDetails> delete = client.getClient().resource(flinkDeployment).delete();

    }

    @Test
    public void sparkOperatorTest() {
        SparkApplication sparkApplication = new SparkApplication();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("test-spark-operator");
        metadata.setNamespace("spark-operator");
        sparkApplication.setMetadata(metadata);

        SparkPodSpec driver =
                SparkPodSpec.Builder()
                        .cores(1)
                        .memory("1G")
                        .serviceAccount("my-release-spark")
                        .build();

        SparkPodSpec executor =
                SparkPodSpec.Builder()
                        .cores(1)
                        .instances(1)
                        .memory("1G")
                        .build();

        Map<String, String> sparkConfMap = new HashMap<>();
//        sparkConfMap.put(
//                SparkConfiguration.SPARK_KUBERNETES_FILE_UPLOAD_PATH().key(),
//                sparkConfig.getK8sFileUploadPath());

        SparkApplicationSpec sparkApplicationSpec =
                SparkApplicationSpec.Builder()
                        .type("Scala")
                        .mode("cluster")
                        .image("https://archive.apache.org/dist/spark/spark-3.1.1")
                        .imagePullPolicy("Always")
                        .mainClass("org.apache.spark.examples.SparkPi")
                        .mainApplicationFile("local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar")
                        .sparkVersion("3.1.1")
                        .restartPolicy(new RestartPolicy("Never"))
                        .driver(driver)
                        .executor(executor)
                        .sparkConf(sparkConfMap)
                        .build();


        sparkApplication.setSpec(sparkApplicationSpec);

        SparkApplication application = client.getClient().resource(sparkApplication).create();
    }


    @Test
    public void deleteSparkApplication() {
        SparkApplication sparkApplication = new SparkApplication();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("test-spark-operator");
        metadata.setNamespace("spark-operator");
        sparkApplication.setMetadata(metadata);


        List<StatusDetails> delete = client.getClient().resource(sparkApplication).delete();

    }
}
