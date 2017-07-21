package com.hexun.rocketmq.client.test;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by yuanyue on 2017/7/8.
 */
public class TestListener implements MessageListenerOrderly {
    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
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
        return ConsumeOrderlyStatus.SUCCESS;
    }
}
