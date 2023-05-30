package com.jm.dispatch;

import lombok.Data;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/17 20:10
 */
@Data
public class DispatchContext {

    private String dispatchType;

    /**
     * 唯一标识
     */
    private String Uid;

    private String dispatchParameters;

    private String executePath;

    private String logStorageType;
}
