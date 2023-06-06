package com.jm.digital.intelligence.engine.facade;

import com.jm.digital.intelligence.engine.model.ResResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/5 16:44
 */
@Service
public class APIFacade {


    public ResResult<List<Map<String,String>>> APIQuery(String apiPath, Map<String,String> paramsMap){

        /**
         * 1.token校验
         * 2.转换请求参数
         */
        //token再次校验，防止请求被串改
        if(!checkToken(paramsMap.get("token"))){
            return ResResult.error("token 校验不通过");
        }
        //todo,数据源信息放到第三方，通过api的唯一标识获取
        String apiKey = paramsMap.get("APIKey");

        /**
         * 1.解密apiKey
         * 2.根据解密之后的apiKey获取appId
         * 3.根据appId获取apiMeta信息
         */

        /**
         * 1.获取请求参数、响应参数
         * 2.根据apiMeta获取sqlCode
         * 3.根据请求响应参数重新构建sqlCode
         */
        String input = paramsMap.get("inputParams");
        String output = paramsMap.get("outputParams");

        /**
         * 1.获取数据源信息
         * 2.sqlCode 解析、校验
         * 3.spi机制获取数据源处理类
         * 4.调用执行逻辑，进行数据查询
         */


        return null;
    }

    private boolean checkToken(String token) {

        return true;
    }

}
