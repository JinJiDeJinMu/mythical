package com.jm.dispatch.python.param;


import com.jm.model.ResourceInfo;
import com.jm.param.DispatchParameters;
import org.apache.commons.lang3.StringUtils;

import java.util.List;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/30 11:05
 */
public class PythonParameters extends DispatchParameters {

    List<ResourceInfo> resourceInfoList;

    @Override
    public boolean checkParameters() {
        return StringUtils.isNotEmpty(getCode()) ;
    }

    public void setResourceInfoList(List<ResourceInfo> resourceInfoList) {
        this.resourceInfoList = resourceInfoList;
    }

    public List<ResourceInfo> getResourceInfoList() {
        return resourceInfoList;
    }
}
