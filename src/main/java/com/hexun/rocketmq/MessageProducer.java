package com.hexun.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.Serializable;


public interface MessageProducer {

    /**
     * 这里很重要：一定要把消息上抛,用于业务代码的事务控制或重试
     *
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    <T> SendResult send(String key, T messageObject) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步消息
     *
     * @param key
     * @param messageObject
     * @param sendCallback
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    <T> void send(String key, T messageObject, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException;

    MQProducer getMQProducer();

}