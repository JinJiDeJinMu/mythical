package com.jm.demo.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.serialization.Serialization;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/26 10:51
 */
public class ActorConf {
    public static final String AKKA_REMOTE_NETTY_TCP_HOSTNAME = "akka.remote.netty.tcp.hostname";
    public static ActorSystem actorSystem;

    public static void init() throws UnknownHostException {
        String hostname = InetAddress.getLocalHost().getHostName();
        Config config = ConfigFactory.parseString(AKKA_REMOTE_NETTY_TCP_HOSTNAME + "=" + hostname);
        actorSystem = ActorSystem.create("jinmu", config.withFallback(ConfigFactory.load()));

        ActorRef testActor = actorSystem.actorOf(Props.create(TestActor.class), getActorRefName(TestActor.class));
        String path = Serialization.serializedActorPath(testActor);
        System.out.println(path);

        ActorRef sendActor = actorSystem.actorOf(Props.create(SendActor.class,testActor), getActorRefName(SendActor.class));
        sendActor.tell("abc",ActorRef.noSender());

        //actorSelection 发送消息 path可以通过Serialization.serializedActorPath(testActor);获取
//        ActorSelection selection = actorSystem.actorSelection(path);
//        selection.tell("测试selection11111", ActorRef.noSender());


        actorSystem.terminate();

//        actorSystem.scheduler().schedule(
//                FiniteDuration.apply(30L, TimeUnit.SECONDS),
//                FiniteDuration.apply(300L, TimeUnit.SECONDS),
//                testActor,
//                "hello,test",
//                actorSystem.dispatcher(),
//                ActorRef.noSender());
    }

    public static String getActorRefName(Class clazz) {
        return clazz.getSimpleName();
    }
}
