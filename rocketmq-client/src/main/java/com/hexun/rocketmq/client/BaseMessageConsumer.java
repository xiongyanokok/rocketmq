package com.hexun.rocketmq.client;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

public abstract class BaseMessageConsumer {


    private String topic;

    private String tag;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public static final String TAG_ALL = "*";


    /**
     * 消费消息
     *
     * @param msg
     */
    public abstract ConsumeOrderlyStatus consume(MessageExt msg);
}
