package com.jm.mythical.k8s;

import com.jm.mythical.k8s.config.K8sClientConfig;
import com.jm.mythical.k8s.flink.operator.listener.TestingListener;
import com.jm.mythical.k8s.service.IK8sDeploymentService;
import com.jm.mythical.k8s.service.IK8sPodService;
import com.jm.mythical.k8s.spark.operator.crds.RestartPolicy;
import com.jm.mythical.k8s.spark.operator.crds.SparkApplication;
import com.jm.mythical.k8s.spark.operator.crds.SparkApplicationSpec;
import com.jm.mythical.k8s.spark.operator.crds.SparkPodSpec;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
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
        FlinkDeployment flinkDeployment = getFlinkDeployment("basic-example", "flink-operator");

        // FlinkDeploymentSpec
        FlinkDeploymentSpec flinkDeploymentSpec = new FlinkDeploymentSpec();
        flinkDeploymentSpec.setFlinkVersion(FlinkVersion.v1_16);
        flinkDeploymentSpec.setImage("flink:1.16");
        Map<String, String> flinkConfiguration = new HashMap<>();
        flinkConfiguration.put("taskmanager.numberOfTaskSlots", "1");
        /**
         * checkpoints
         */
        flinkConfiguration.put("state.checkpoints.dir", "file:///opt/flink/checkpoints");
        /**
         * HA
         */
        flinkConfiguration.put("high-availability.type", "kubernetes");
        flinkConfiguration.put("high-availability", "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory");
        flinkConfiguration.put("high-availability.storageDir", "file:///opt/flink/flink_recovery");

        flinkDeploymentSpec.setFlinkConfiguration(flinkConfiguration);
        flinkDeployment.setSpec(flinkDeploymentSpec);
        flinkDeploymentSpec.setServiceAccount("flink-operator");
        JobManagerSpec jobManagerSpec = new JobManagerSpec();
        jobManagerSpec.setResource(new Resource(1.0, "1024m", "2G"));
        flinkDeploymentSpec.setJobManager(jobManagerSpec);
        TaskManagerSpec taskManagerSpec = new TaskManagerSpec();
        taskManagerSpec.setResource(new Resource(1.0, "1024m", "2G"));
        flinkDeploymentSpec.setTaskManager(taskManagerSpec);

        /**
         * flink pod template
         */
        Pod podTemplate = new Pod();
        podTemplate.setSpec(getPodSpec());

        flinkDeploymentSpec.setPodTemplate(podTemplate);

        flinkDeploymentSpec
                .setJob(
                        JobSpec.builder()
                                .jarURI(
                                        "local:///opt/flink/examples/streaming/StateMachineExample.jar")
                                .parallelism(1)
                                .upgradeMode(UpgradeMode.STATELESS)
                                .build());


        FlinkDeployment deployment = client.getClient().resource(flinkDeployment).create();

    }

    public PodSpec getPodSpec() {
        PodSpec podSpec = new PodSpec();
        List<Container> containers = new ArrayList<>();
        containers.add(getPodSpecContainer());

        podSpec.setContainers(containers);

        //volumes
        List<Volume> volumes = new ArrayList<>();
        //log-volume
        Volume log_volume = new Volume();
        log_volume.setName("log-volume");
        log_volume.setPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource("flink-log-pvc ", false));

        //ck-volume
        Volume ck_volume = new Volume();
        ck_volume.setName("checkpoints-volume");
        ck_volume.setPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource("flink-ck-pvc", false));

        //ha-volume
        Volume ha_volume = new Volume();
        ha_volume.setName("ha-volume");
        ha_volume.setPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource("flink-ha-pvc", false));

        volumes.add(log_volume);
        volumes.add(ck_volume);
        volumes.add(ha_volume);

        podSpec.setVolumes(volumes);

        return podSpec;
    }

    public Container getPodSpecContainer() {
        Container container = new Container();
        //env
        List<EnvVar> envs = new ArrayList<>();
        EnvVar envVar = new EnvVar();
        envVar.setAdditionalProperty("TZ", "Asia/Shanghai");
        envs.add(envVar);

        container.setEnv(envs);
        // volumeMounts
        List<VolumeMount> volumeMounts = new ArrayList<>();

        VolumeMount log = new VolumeMount();
        log.setName("log-volume");
        log.setMountPath("/opt/flink/log");

        VolumeMount ck = new VolumeMount();
        ck.setName("checkpoints-volume");
        ck.setMountPath("/opt/flink/checkpoints");

        VolumeMount ha = new VolumeMount();
        ha.setName("ha-volume");
        ha.setMountPath("/opt/flink/ha");

        volumeMounts.add(log);
        volumeMounts.add(ha);
        volumeMounts.add(ck);

        container.setVolumeMounts(volumeMounts);

        return container;
    }

    FlinkDeployment getFlinkDeployment(String name, String namespace) {
        FlinkDeployment flinkDeployment = new FlinkDeployment();
        flinkDeployment.setApiVersion("flink.apache.org/v1beta1");
        flinkDeployment.setKind("FlinkDeployment");
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setNamespace(namespace);
        objectMeta.setName(name);

        return flinkDeployment;
    }

    @Test
    public void deleteFlinkDeployment() {

        List<StatusDetails> delete = client.getClient().resource(getFlinkDeployment("basic-example", "flink-operator")).delete();

    }


    @Test
    public void testFlinkDeploymentListener(com.jm.mythical.k8s.flink.operator.FlinkDeployment flinkDeployment) {
        TestingListener listener1 = new TestingListener();
        TestingListener listener2 = new TestingListener();
        ArrayList<FlinkResourceListener> listeners = new ArrayList<>();
        listeners.add(listener1);
        listeners.add(listener2);

        StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder =
                StatusRecorder.create(client.getClient(), new MetricManager<>(), listeners);
        EventRecorder eventRecorder = EventRecorder.create(client.getClient(), listeners);

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
                        .image("apache/spark:a3.3.1")
                        .imagePullPolicy("Always")
                        .mainClass("org.apache.spark.examples.SparkPi")
                        .mainApplicationFile("local:///opt/spark/examples/jars/spark-examples_2.12-3.3.1.jar")
                        .sparkVersion("3.3.1")
                        .restartPolicy(new RestartPolicy("Never"))
                        .driver(driver)
                        .executor(executor)
                        .sparkConf(sparkConfMap)
                        .build();


        sparkApplication.setSpec(sparkApplicationSpec);

        SparkApplication application = client.getClient().resource(sparkApplication).create();

        testWatch();
    }

    @Test
    public void testWatch() {
        SparkApplication sparkApplication = new SparkApplication();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("test-spark-operator");
        metadata.setNamespace("spark-operator");
        sparkApplication.setMetadata(metadata);

        client.getClient().pods().inNamespace("spark-operator").withName("test-spark-operator-driver").watch(new Watcher<Pod>() {
            @Override
            public void eventReceived(Action action, Pod pod) {
                System.out.println("action = " + action);
                System.out.println("pod = " + pod);
            }

            @Override
            public void onClose(WatcherException e) {

            }
        });
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
