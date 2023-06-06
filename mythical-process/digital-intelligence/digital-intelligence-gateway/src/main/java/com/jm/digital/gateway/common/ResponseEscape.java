package com.jm.digital.gateway.common;

import cn.hutool.json.JSONUtil;
import com.jm.digital.gateway.model.ResResult;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/3 14:31
 */
public class ResponseEscape {

    public static Mono<Void> returnMessage(ServerWebExchange exchange, Object message){
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.OK);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        ResResult.error(message.toString());
        DataBuffer dataBuffer = response.bufferFactory().allocateBuffer().write(JSONUtil.toJsonStr(ResResult.error(message.toString())).getBytes(StandardCharsets.UTF_8));
        return response.writeWith(Mono.just(dataBuffer));
    }

}
