package com.hexun.rocketmq.client;

import com.hexun.common.utils.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

public class MessageConsumer extends DefaultMQPushConsumer implements DisposableBean {

    /**
     * logger
     */
    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    /**
     * topic
     */
    String topic;

    /**
     * sub expression
     */
    String subExpression;

    /**
     * 消息消费 Listener
     */
    MessageListener messageListener;

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
    public void setMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
    }

    /**
     * 初始化
     * 默认的 consumer group name :"CG-" + topic
     *
     * @throws MQClientException
     */
    public void init() throws MQClientException {
        if (StringUtils.isEmpty(getConsumerGroup()) || "DEFAULT_CONSUMER".equals(getConsumerGroup())) {
            setConsumerGroup("CG-" + topic);
        }
        subscribe(topic, subExpression);
        setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        registerMessageListener(messageListener);
        setVipChannelEnabled(false);
        start();
        log.debug("消费者启动:TOPIC={},消费者ConsumerGroup={}", topic, getConsumerGroup());
    }

    @Override
    public void destroy() throws Exception {
        unsubscribe(topic);
        shutdown();
    }
}