package com.hexun.rocketmq.client;

import org.apache.rocketmq.client.ClientConfig;

/**
 * 基于topic的配置信息
 */
public class TopicConfig extends ClientConfig {
    /**
     * topic
     */
    private String topic;

    /**
     * 获取topic
     * @return
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 设置topic
     * @param topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void init() {

    }
}
