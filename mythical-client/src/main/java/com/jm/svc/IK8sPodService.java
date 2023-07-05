package com.jm.svc;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.dsl.LogWatch;

import java.io.OutputStream;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/20 16:06
 */
public interface IK8sPodService {

    PodList list(String namespace);

    Pod create(String namespace, PodBuilder podBuilder);

    Pod create(String namespace, String podName, String path);

    Pod get(String namespace, String podName);

    Pod edit(String namespace, String podName, PodBuilder podBuilder);

    List<StatusDetails> delete(String namespace, String podName);

    String log(String namespace, String podName);

    LogWatch logWatch(String namespace, String podName, Integer tailLine, OutputStream outputStream);
}