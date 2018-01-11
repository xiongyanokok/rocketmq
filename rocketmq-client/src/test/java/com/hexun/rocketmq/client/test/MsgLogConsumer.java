package com.hexun.rocketmq.client.test;

import com.hexun.common.utils.DateUtils;
import com.hexun.rocketmq.client.BaseMessageConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;


public class MsgLogConsumer extends BaseMessageConsumer {

    @Override
    public boolean consume(MessageExt msg){
        String body = null;
        try {
            body = new String(msg.getBody(),"UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println("MsgLogConsumer**********" + DateUtils.now());
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }
}
