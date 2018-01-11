package com.hexun.rocketmq.client;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ExecutorService;

public abstract class BaseMessageConsumer {


    /**
     * topic
     */
    private String topic;

    /**
     * tag
     */
    private String tag;

    /**
     * 获取topic
     *
     * @return String
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 设置topic
     *
     * @param topic String
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * 获取tag
     *
     * @return String
     */
    public String getTag() {
        return tag;
    }

    /**
     * 设置tag
     *
     * @param tag String
     */
    public void setTag(String tag) {
        this.tag = tag;
    }

    /**
     * all tag
     */
    static final String TAG_ALL = "*";


    /**
     * 消费消息
     *
     * @param msg MessageExt
     * @return 是否消费成功
     */
    public abstract boolean consume(MessageExt msg);
}
