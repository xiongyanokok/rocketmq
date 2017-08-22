package com.hexun.rocketmq.client;

import com.hexun.common.utils.IpUtils;
import com.hexun.common.utils.JsonUtils;
import com.hexun.common.utils.StringUtils;
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

import java.nio.charset.Charset;

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
     * 健康检查
     */
    private HealthChecker checker;

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
        if (StringUtils.isBlank(getProducerGroup()) || "DEFAULT_PRODUCER".equals(getProducerGroup())) {
            setProducerGroup("PG-" + topic);
        }
        if (getSendMsgTimeout() <= 0) {
            setSendMsgTimeout(10000);
        }
        if (getRetryTimesWhenSendFailed() <= 0) {
            setRetryTimesWhenSendFailed(1);
        }
        setClientIP(IpUtils.getHostIP());
        setCreateTopicKey(topic);
        start();
        checker = new HealthChecker(this);
        log.info("ROCKETMQ Producer {} start , IP = ", getProducerGroup(), getClientIP());
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
     * @return
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws MQBrokerException    MQBrokerException
     * @throws InterruptedException InterruptedException
     */
    public SendResult send(String key, Object messageObject) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(key, messageObject, "");
    }


    /**
     * send sync with tag
     *
     * @param key           消息的主键,以便后续查询
     * @param messageObject 消息体
     * @param tag           标签
     * @return
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws MQBrokerException    MQBrokerException
     * @throws InterruptedException InterruptedException
     */
    public SendResult send(String key, Object messageObject, String tag) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return send(getTopic(), key, messageObject, tag);
    }


    public SendResult send(String topic, String key, Object messageObject, String tag) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        byte[] msgBytes = JsonUtils.obj2Bytes(messageObject);

        Message msg = new Message(topic,// topic
                tag,// tag
                key,// keys
                msgBytes
        );

        SendResult sendResult = send(msg);
        log.info("\n\nMSG={}\nSEND RESULT{}", new String(msgBytes, Charset.forName("UTF-8")), sendResult);
        return sendResult;
    }


    public void send(String key, Object messageObject, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(key, messageObject, "", sendCallback);
    }

    /**
     * send async with tag
     *
     * @param key           消息的主键,以便后续查询
     * @param messageObject 消息体
     * @param tag           标签
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws InterruptedException InterruptedException
     */
    public void send(String key, Object messageObject, String tag, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
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
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void sendAsync(String key, Object messageObject, String tag) throws MQClientException, RemotingException, InterruptedException {
        send(key, messageObject, tag, createCallback(messageObject));
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
        log.info("rocketmq send one way key={}", key);
    }

    /**
     * 创建异步回调信息
     *
     * @param message
     * @return
     */
    private SendCallback createCallback(final Object message) {
        return new SendCallback() {
            /**
             * message 内容
             */
            String msg = JsonUtils.obj2String(message);

            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("\n\nrocketmq send msg={}\nmsgId={}\nstatus={}", msg, sendResult.getMsgId(), sendResult.getSendStatus());
            }

            @Override
            public void onException(Throwable e) {
                log.error("\n\nrocketmq send msg={}\n发送消息错误", msg, e);
            }
        };
    }
}
