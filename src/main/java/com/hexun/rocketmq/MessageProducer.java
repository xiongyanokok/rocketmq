package com.hexun.rocketmq;

import com.hexun.common.utils.JsonUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

/**
 * 消息生产者
 */
public class MessageProducer extends DefaultMQProducer implements DisposableBean {

    /**
     * logger
     */
    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    /**
     * topic 必须设置
     */
    private String topic;

    /**
     * 设置 topic
     *
     * @param topic topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * 获取 topic
     *
     * @return topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 初始化
     *
     * @throws MQClientException
     */
    public void init() throws MQClientException {
        setVipChannelEnabled(false);
        setProducerGroup("PG-" + topic);
        start();
        log.info("{} start", getClientIP());
    }

    /**
     * 销毁
     *
     * @throws Exception 关闭时的异常
     */
    @Override
    public void destroy() throws Exception {
        this.shutdown();
    }

    /**
     * send sync with tag
     *
     * @param key           消息的主键,以便后续查询
     * @param messageObject 消息体
     * @param <T>           消息体bean类型
     * @return
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws MQBrokerException    MQBrokerException
     * @throws InterruptedException InterruptedException
     */
    public <T> SendResult send(String key, T messageObject) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(key, messageObject, "");
    }


    /**
     * send sync with tag
     *
     * @param key           消息的主键,以便后续查询
     * @param messageObject 消息体
     * @param tag           标签
     * @param <T>           消息体bean类型
     * @return
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws MQBrokerException    MQBrokerException
     * @throws InterruptedException InterruptedException
     */
    public <T> SendResult send(String key, T messageObject, String tag) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Message msg = new Message(getTopic(),// topic
                tag,// tag
                key,// keys
                JsonUtils.obj2Bytes(messageObject)
        );

        SendResult sendResult = send(msg);
        log.info("rocketmq send success,key={};sendResult:{}", key, sendResult);
        return sendResult;
    }


    public <T> void send(String key, T messageObject, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(key, messageObject, "", sendCallback);
    }

    /**
     * send async with tag
     *
     * @param key           消息的主键,以便后续查询
     * @param messageObject 消息体
     * @param tag           标签
     * @param <T>           消息体bean类型
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws InterruptedException InterruptedException
     */
    public <T> void send(String key, T messageObject, String tag, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        Message msg = new Message(getTopic(),// topic
                tag,// tag
                key,// keys
                JsonUtils.obj2Bytes(messageObject)
        );

        send(msg, sendCallback);
        log.info("rocketmq send key={}", key);
    }

    /**
     * 同步发送,异步回调,回调设置为黑洞.
     *
     * @param key           消息的主键,以便后续查询
     * @param messageObject 消息体
     * @param tag           标签
     * @param <T>           消息体bean类型
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public <T> void sendAsync(String key, T messageObject, String tag) throws MQClientException, RemotingException, InterruptedException {
        send(key, messageObject, tag, defaultSendCallback);
    }

    /**
     * 单方发送
     *
     * @param key           消息的主键,以便后续查询
     * @param messageObject 消息体
     * @param tag           标签
     * @param <T>           消息体bean类型
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws InterruptedException InterruptedException
     */
    public <T> void sendOneway(String key, T messageObject, String tag) throws MQClientException, RemotingException, InterruptedException {
        Message msg = new Message(getTopic(),// topic
                tag,// tag
                key,// keys
                JsonUtils.obj2Bytes(messageObject)
        );

        sendOneway(msg);
        log.info("rocketmq sendOneway key={}", key);
    }

    SendCallback defaultSendCallback = new SendCallback() {
        @Override
        public void onSuccess(SendResult sendResult) {
            log.info("msgId={},status={}", sendResult.getMsgId(), sendResult.getSendStatus());
        }

        @Override
        public void onException(Throwable e) {
            log.error("发送消息错误", e);
        }
    };
}
