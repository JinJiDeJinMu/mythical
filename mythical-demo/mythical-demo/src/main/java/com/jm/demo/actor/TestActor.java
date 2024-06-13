package com.jm.demo.actor;

import akka.actor.ActorRef;
import akka.actor.UntypedAbstractActor;
import akka.serialization.Serialization;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/26 10:57
 */
public class TestActor extends UntypedAbstractActor {

    @Override
    public void onReceive(Object message) throws Throwable, Throwable {
        String path = Serialization.serializedActorPath(getSender());
        System.out.println(path);
        System.out.println("收到消息 = " + message);

        System.out.println("----------");

    }
}
