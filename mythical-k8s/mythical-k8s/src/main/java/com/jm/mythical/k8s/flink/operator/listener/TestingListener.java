package com.jm.mythical.k8s.flink.operator.listener;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 * <p>
 * 1.Implement the interface
 * 2.Add your listener class to org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener in META-INF/services
 * 3.Package your JAR and add it to the plugins directory of your operator image (/opt/flink/plugins)
 *
 * @Author jinmu
 * @Date 2023/7/31 13:50
 */
public class TestingListener implements FlinkResourceListener {

    public List<StatusUpdateContext<?, ?>> updates = new ArrayList<>();
    public List<ResourceEventContext<?>> events = new ArrayList<>();
    public Configuration config;

    public void onStatusUpdate(StatusUpdateContext<?, ?> ctx) {
        updates.add(ctx);
    }

    public void onEvent(ResourceEventContext<?> ctx) {
        events.add(ctx);
    }

    @Override
    public void onDeploymentStatusUpdate(StatusUpdateContext<FlinkDeployment, FlinkDeploymentStatus> statusUpdateContext) {
        onStatusUpdate(statusUpdateContext);
    }

    @Override
    public void onDeploymentEvent(ResourceEventContext<FlinkDeployment> resourceEventContext) {
        onEvent(resourceEventContext);
    }

    @Override
    public void onSessionJobStatusUpdate(StatusUpdateContext<FlinkSessionJob, FlinkSessionJobStatus> statusUpdateContext) {
        onStatusUpdate(statusUpdateContext);
    }

    @Override
    public void onSessionJobEvent(ResourceEventContext<FlinkSessionJob> resourceEventContext) {
        onEvent(resourceEventContext);
    }

    @Override
    public void configure(Configuration config) {
        this.config = config;
    }
}
