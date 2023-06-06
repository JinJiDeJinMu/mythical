package com.jm.digital.intelligence.engine.controller;

import com.jm.digital.intelligence.engine.common.ParametersConfig;
import com.jm.digital.intelligence.engine.facade.APIFacade;
import com.jm.digital.intelligence.engine.model.ResResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/5 15:09
 */
@RequestMapping(value = "/openAPI/v1")
public class APIController {

    @Autowired
    APIFacade apiFacade;

    @RequestMapping("/{apiPath}")
    public ResResult<List<Map<String,String>>> apiQuery(@PathVariable String apiPath, HttpServletRequest request){
        return apiFacade.APIQuery(apiPath, ParametersConfig.transformMap(request));
    }
}
