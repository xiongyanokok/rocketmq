package com.hexun.rocketmq.client;

import com.hexun.common.utils.IpUtils;
import com.hexun.common.utils.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
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
    private String topic;

    /**
     * sub expression
     */
    private String subExpression;

    /**
     * listener Class
     */
    private String listenerClass;

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
     * @param subExpression String
     */
    public void setSubExpression(String subExpression) {
        this.subExpression = subExpression;
    }

    /**
     * 设置setListenerClass
     *
     * @param listenerClass
     */
    public void setListenerClass(String listenerClass) {
        this.listenerClass = listenerClass;
        if (StringUtils.isNotBlank(listenerClass)) {
            try {
                Class clazz = Class.forName(this.listenerClass);
                Object listener = clazz.newInstance();
                if (listener instanceof MessageListener) {
                    setMessageListener((MessageListener) listener);
                } else {
                    log.error("listenerClass {} is not instance of MessageListener ", this.listenerClass);
                }
            } catch (ClassNotFoundException e) {
                log.error("listener class {} not found", this.listenerClass, e);
            } catch (IllegalAccessException | InstantiationException e) {
                log.error("listener class instance {} error", this.listenerClass, e);
            }
        }
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
        if (getMessageListener() == null) {
            log.error("no message listener found here");
            return;
        }
        if (getMessageListener() instanceof MessageListenerOrderly) {
            registerMessageListener((MessageListenerOrderly) getMessageListener());
        } else if (getMessageListener() instanceof MessageListenerConcurrently) {
            registerMessageListener((MessageListenerConcurrently) getMessageListener());
        }
        setMessageModel(MessageModel.CLUSTERING);
        setClientIP(IpUtils.getHostIP());
        setVipChannelEnabled(false);
        start();
        log.info("服务器={}\n" +
                "TOPIC={}\n" +
                "subExpression={}\n" +
                "消费者ConsumerGroup={}\n" +
                "listener class={}\n" +
                "客户端IP={}", getNamesrvAddr(), topic, subExpression, getConsumerGroup(), getMessageListener().getClass(), getClientIP());
    }

    @Override
    public void destroy() throws Exception {
        unsubscribe(topic);
        shutdown();
    }
}