package com.jm.param;

import lombok.Data;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 20:40
 */
@Data
public abstract class DispatchParameters implements Parameters{

    private String runParameters;

    private String envParameters;

    private String extraParameters;

    private String code;
}
