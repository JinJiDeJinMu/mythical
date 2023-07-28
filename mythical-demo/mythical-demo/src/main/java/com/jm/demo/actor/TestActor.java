package com.jm.demo.actor;

import akka.actor.UntypedAbstractActor;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/26 10:57
 */
public class TestActor extends UntypedAbstractActor {

    @Override
    public void onReceive(Object message) throws Throwable, Throwable {
        System.out.println("收到消息 = " + message);
    }
}
