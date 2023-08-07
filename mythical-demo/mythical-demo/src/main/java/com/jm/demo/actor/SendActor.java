package com.jm.demo.actor;

import akka.actor.ActorRef;
import akka.actor.UntypedAbstractActor;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/26 13:44
 */
public class SendActor extends UntypedAbstractActor {

    private ActorRef receiver;

    public SendActor(ActorRef receiver) {
        this.receiver = receiver;
    }

    public void send(String message) {
        this.receiver.tell(message, getSelf());
    }

    @Override
    public void onReceive(Object message) throws Throwable, Throwable {
        System.out.println("send = " + message);

        send(message.toString());
        getContext().stop(getSelf());
    }

}
