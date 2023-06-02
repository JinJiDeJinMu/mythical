package com.jm.spline;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * TODO ExecutionEvent
 *
 * @Author jinmu
 * @Date 2023/3/4 11:31
 */
@Data
public class ExecutionEvent {

    private String planId;
    private long timestamp;
    private long durationNs;
    private String discriminator;
    private Object error;
    private Map<String, Object> extra;
    private Map<String, List<String>> labels;
}