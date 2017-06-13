package com.hexun.rocketmq.impl;

import com.hexun.common.utils.JsonUtils;
import com.hexun.rocketmq.MessageProducer;
import com.hexun.rocketmq.client.ProducerClientConfig;
import com.hexun.rocketmq.utils.RunTimeUtil;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class MessageProducerImpl implements DisposableBean, MessageProducer {

    /**
     * logger
     */
    private static final Logger log = LoggerFactory.getLogger(MessageProducerImpl.class);

    /**
     * 最大可发20M消息
     */
    private int maxMessageSize = 1 * 1024 * 1024;

    /**
     * 生产者实例
     */
    private DefaultMQProducer producer;

    @Autowired
    ProducerClientConfig config;

    @Override
    public void destroy() throws Exception {
        producer.shutdown();
    }

    @PostConstruct
    public synchronized void initProducer() {
        try {
            producer = new DefaultMQProducer("GROUP" + config.getTopic());
            producer.setNamesrvAddr(config.getNamesrvAddr());
            producer.setMaxMessageSize(maxMessageSize);
            producer.setInstanceName(RunTimeUtil.getRocketMqUniqeInstanceName());
            producer.setVipChannelEnabled(config.isVipChannelEnabled());
            producer.start();
        } catch (MQClientException e) {
            log.error("rocketmq restartProducer error ", e);
        }
    }

    @Override
    public <T> SendResult send(String key, T messageObject) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Message msg = new Message(config.getTopic(),// topic
                config.getTag(),// tag
                key,// keys
                JsonUtils.obj2Bytes(messageObject)
        );

        SendResult sendResult = producer.send(msg);
        log.info("rocketmq send success,key={};sendResult:{}", key, sendResult);
        return sendResult;
    }

    @Override
    public <T> void send(String key, T messageObject, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        Message msg = new Message(config.getTopic(),// topic
                config.getTag(),// tag
                key,// keys
                JsonUtils.obj2Bytes(messageObject)
        );

        producer.send(msg, sendCallback);
        log.info("rocketmq send key={}", key);
    }

    public MQProducer getMQProducer() {
        return producer;
    }

}