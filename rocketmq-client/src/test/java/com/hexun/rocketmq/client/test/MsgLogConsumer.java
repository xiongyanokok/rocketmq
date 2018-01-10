package com.hexun.rocketmq.client.test;

import com.hexun.rocketmq.client.BaseMessageConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;

@Component
public class MsgLogConsumer extends BaseMessageConsumer {
    @Override
    public String getTopic() {
        return "yuanyue";
    }

    @Override
    public String getTag() {
        return "*";
    }

    @Override
    public ConsumeOrderlyStatus consume(MessageExt msg){
        String body = null;
        try {
            body = new String(msg.getBody(),"UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println(body);
        return null;
    }
}
