package com.jm.mythical.k8s;

import com.jm.mythical.k8s.config.K8sClientConfig;
import com.jm.mythical.k8s.service.IK8sDeploymentService;
import com.jm.mythical.k8s.service.IK8sPodService;
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
    public void operatorTest() {
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
}
