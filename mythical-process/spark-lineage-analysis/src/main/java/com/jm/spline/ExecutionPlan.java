package com.jm.spline;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * TODO ExecutionPlan
 *
 * @Author jinmu
 * @Date 2023/3/4 11:31
 */
@Data
public class ExecutionPlan {

    private String id;
    private String name;
    private Operations operations;
    private List<Attributes> attributes;
    private Expressions expressions;
    private SystemInfo systemInfo;
    private AgentInfo agentInfo;
    private Map<String, Object> extraInfo;
    private String discriminator;
    private Map<String, List<String>> labels;

}