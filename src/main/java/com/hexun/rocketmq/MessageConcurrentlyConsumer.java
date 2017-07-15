package com.hexun.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import javax.annotation.PostConstruct;

public class MessageConcurrentlyConsumer extends DefaultMQPushConsumer implements DisposableBean {

    /**
     * logger
     */
    private static final Logger log = LoggerFactory.getLogger(MessageOrderlyConsumer.class);

    /**
     * topic
     */
    String topic;

    /**
     * sub expression
     */
    String subExpression;

    /**
     * 顺序消息消费 Listener
     */
    MessageListenerConcurrently messageListener;

    /**
     * 设置 topic
     *
     * @param topic topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * 设置setSubExpression
     *
     * @param subExpression
     */
    public void setSubExpression(String subExpression) {
        this.subExpression = subExpression;
    }

    /**
     * 设置 listener
     *
     * @param messageListener
     */
    public void setMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
    }

    /**
     * 初始化
     * @throws MQClientException
     */
    public void init() throws MQClientException {
        setConsumerGroup("CG-" + topic);
        subscribe(topic, subExpression);
        registerMessageListener(messageListener);
        start();
    }

    @Override
    public void destroy() throws Exception {
        super.shutdown();
    }
}