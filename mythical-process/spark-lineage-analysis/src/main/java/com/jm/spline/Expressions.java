package com.jm.spline;

import lombok.Data;

import java.util.List;

@Data
public class Expressions {

    private List<Functions> functions;
    private List<Functions> constants;

}