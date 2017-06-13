package com.hexun.rocketmq;

import org.apache.rocketmq.common.message.MessageExt;

import java.io.Serializable;


public interface MessageExtConsumer<E extends Serializable> extends MessageConsumer<E> {

    void consume(MessageExt msgExt, E body) throws Exception;
}