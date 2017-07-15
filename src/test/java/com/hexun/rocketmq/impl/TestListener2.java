package com.hexun.rocketmq.impl;

import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by yuanyue on 2017/7/8.
 */
public class TestListener2 implements MessageListenerConcurrently {

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msgext : msgs) {
                try {
                    String body = new String(msgext.getBody(),"UTF-8");
                    System.out.println(body);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
