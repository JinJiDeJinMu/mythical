package com.jm.digital.gateway.filter;

import com.google.common.collect.Lists;
import com.jm.digital.gateway.common.ResponseEscape;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * TODO
 * 限流拦截器
 * @Author jinmu
 * @Date 2023/6/3 14:15
 */
@Slf4j
@Component
@RefreshScope
public class RateLimitFilter implements GlobalFilter, Ordered {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedisScript<Long> redisScript;

    //@Value("${jm.rateLimit.percent}")
    private Double percent;

    //@Value("${jm.rateLimit.resetBucketInterval}")
    private long resetBucketInterval;


    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        log.info("rateLimit is start");

        Map<String, Object> attributes = exchange.getAttributes();
        //通常从请求头中获取
        String apiKey = "";
        Integer totalQps = null;

        if(isAllowed(apiKey,totalQps)){
            return chain.filter(exchange);
        }
        return ResponseEscape.returnMessage(exchange,"请求没有通过限流设置");
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private boolean isAllowed(String id,Integer bucketMaxTokens){
        return stringRedisTemplate.execute(redisScript, buildKeys(id), buildParams(bucketMaxTokens))>=0;
    }
    private List<String> buildKeys(String id) {
        return Arrays.asList("request_rate_limiter.{" + id + "}.tokens");
    }
    private String[] buildParams(Integer bucketMaxTokens){
        long intervalPerPermit = resetBucketInterval / bucketMaxTokens;
        long initTokens = Math.min((new Double(percent * bucketMaxTokens)).longValue(), bucketMaxTokens);
        List<String > list = Lists.newArrayList();
        list.add(String.valueOf(intervalPerPermit));
        list.add(String.valueOf(System.currentTimeMillis()));
        list.add(String.valueOf(initTokens));
        list.add(String.valueOf(bucketMaxTokens));
        list.add(String.valueOf(resetBucketInterval));
        return list.toArray(new String[]{});
    }
}
