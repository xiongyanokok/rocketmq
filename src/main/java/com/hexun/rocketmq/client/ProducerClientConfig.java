package com.hexun.rocketmq.client;

/**
 * 生产者配置
 */
public class ProducerClientConfig extends org.apache.rocketmq.client.ClientConfig {
    /**
     * topic 必须设置
     */
    private String topic;

    /**
     * tag 设置 , 可以同时消费多个标签 tag_a | tag_b | tag_c
     */
    private String tag;

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}
