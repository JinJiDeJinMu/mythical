package com.jm.spline;

import lombok.Data;

import java.util.List;

/**
 * TODO Operations
 *
 * @Author jinmu
 * @Date 2023/3/4 11:31
 */
@Data
public class Operations {

    private Write write;
    private List<Reads> reads;
    private List<Other> other;

}