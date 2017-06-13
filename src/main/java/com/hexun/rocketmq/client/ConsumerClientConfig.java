package com.hexun.rocketmq.client;

import com.hexun.rocketmq.MessageConsumer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yuanyue on 2017/6/9.
 */
public class ConsumerClientConfig extends org.apache.rocketmq.client.ClientConfig {
    /**
     * topic 必须设置
     */
    private String topic;

    /**
     * 消费者Topic 的 tag,以及对应的类
     */
    Map<String, String> consumerMap;


    public String getTopic() {
        return topic;
    }

    public Map<String, String> getConsumerMap() {
        return consumerMap;
    }

    public void setConsumerMap(Map<String, String> consumerMap) {
        this.consumerMap = consumerMap;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
