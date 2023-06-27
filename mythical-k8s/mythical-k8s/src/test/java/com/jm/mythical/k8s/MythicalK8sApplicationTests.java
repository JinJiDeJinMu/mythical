package com.jm.mythical.k8s;

import com.jm.mythical.k8s.service.IK8sPodService;
import io.fabric8.kubernetes.api.model.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class MythicalK8sApplicationTests {

    @Autowired
    IK8sPodService ik8sPodService;

    @Test
    void contextLoads() {
    }


    @Test
    public void getLog() {
        String log = ik8sPodService.log("jinmu", "tomcat-test");
        System.out.println(log);
    }

    @Test
    public void createPod() {
        ObjectMeta metadata = new ObjectMeta();
        metadata.setNamespace("jinmu");
        metadata.setName("tomcat-test");

        List<Container> containers = new ArrayList<>();
        PodSpec podSpec = new PodSpec();

        Container container = new Container();
        container.setName("tomcat");
        container.setImage("tomcat");
        containers.add(container);

        podSpec.setContainers(containers);
        podSpec.setServiceAccount("default");

        PodStatus podStatus = new PodStatus();

        PodBuilder podBuilder = new PodBuilder()
                .withKind("Pod")
                .withSpec(podSpec)
                .withStatus(podStatus);


        Pod pod = ik8sPodService.create("jinmu", podBuilder);
    }

}
