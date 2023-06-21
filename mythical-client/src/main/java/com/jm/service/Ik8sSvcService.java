package com.jm.service;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.StatusDetails;

import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/21 10:51
 */
public interface Ik8sSvcService {

    Service get(String namespace, String SvcName);

    ServiceList list(String namespace);

    Service create(String namespace, String SvcName, ServiceBuilder serviceBuilder);

    List<StatusDetails> delete(String namespace, String SvcName);

    Service edit(String namespace, String ScvName, ServiceBuilder serviceBuilder);
}
