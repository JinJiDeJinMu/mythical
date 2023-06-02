package com.jm.spline;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Functions {

    private String id;
    private String dataType;
    private List<Map<String, String>> childRefs;
    private Map<String, String> extra;
    private String name;
    private Map<String, String> params;

}