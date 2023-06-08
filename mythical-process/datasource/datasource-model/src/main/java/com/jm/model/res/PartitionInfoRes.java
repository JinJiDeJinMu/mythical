package com.jm.model.res;

import lombok.Data;

import java.util.Date;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/4/13 14:23
 */
@Data
public class PartitionInfoRes {

    private Integer index;

    private String name;

    private Long numRows;

    private Long storageSize;

    private Date createTime;

    private Date updateTime;
}
