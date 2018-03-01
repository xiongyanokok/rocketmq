package com.hexun.rocketmq.client;

import com.hexun.common.utils.IpUtils;
import com.hexun.common.utils.JsonUtils;
import com.hexun.common.utils.StringUtils;
import com.hexun.rocketmq.client.utils.ListSplitter;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.List;

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
        //健康检查
        HealthChecker.healthCheck(this);
        log.info("ROCKETMQ Producer {} start , IP = {}", getProducerGroup(), getClientIP());
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
     * build msg
     *
     * @param key           key
     * @param messageObject 对象
     * @return Message
     */
    public Message buildMsg(String key, Object messageObject) {
        return buildMsg(this.topic, key, messageObject, "");
    }

    /**
     * build msg
     *
     * @param key           key
     * @param messageObject 对象
     * @return Message
     */
    public Message buildMsg(String topic, String key, Object messageObject) {
        return buildMsg(topic, key, messageObject, "");
    }

    /**
     * build msg
     *
     * @param key           key
     * @param messageObject byte[] 数组
     * @return Message
     */
    public Message buildBytesMsg(String topic, String key, byte[] messageObject, String tag) {
        return new Message(topic,
                tag,
                key,
                messageObject
        );
    }

    /**
     * build msg
     *
     * @param key           key
     * @param messageObject 对象
     * @return Message
     */
    public Message buildMsg(String topic, String key, Object messageObject, String tag) {
        byte[] msgBytes = JsonUtils.obj2Bytes(messageObject);
        return buildBytesMsg(topic, key, msgBytes, tag);
    }

    /**
     * send sync with key
     *
     * @param key           消息的主键,以便后续查询
     * @param messageObject 消息体
     * @return SendResult
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
     * @return SendResult
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws MQBrokerException    MQBrokerException
     * @throws InterruptedException InterruptedException
     */
    public SendResult send(String key, Object messageObject, String tag) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return send(getTopic(), key, messageObject, tag);
    }


    public SendResult send(String topic, String key, Object messageObject, String tag) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Message msg = buildMsg(topic, key, messageObject, tag);
        SendResult sendResult = send(msg);
        if (log.isInfoEnabled()) {
            log.info("\n\nMSG={}\nSEND RESULT{}", JsonUtils.obj2String(messageObject), sendResult);
        }
        return sendResult;
    }

    public SendResult send(String topic, String key, byte[] messageObject, String tag) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Message msg = buildBytesMsg(topic, key, messageObject, tag);
        SendResult sendResult = send(msg);
        if (log.isInfoEnabled()) {
            log.info("\n\nMSG={}\nSEND RESULT{}", JsonUtils.obj2String(messageObject), sendResult);
        }
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
        Message msg = new Message(getTopic(),
                tag,
                key,
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
     * @throws MQClientException    MQClientException
     * @throws RemotingException    RemotingException
     * @throws InterruptedException InterruptedException
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
        Message msg = new Message(getTopic(),
                tag,
                key,
                JsonUtils.obj2Bytes(messageObject)
        );

        sendOneway(msg);
        log.info("rocketmq send one way key={}", key);
    }

    /**
     * 创建异步回调信息
     *
     * @param message 消息体
     * @return SendCallback
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


    /**
     * 顺序发消息,默认发送到第一个queue
     *
     * @param key           key
     * @param messageObject 消息对象
     * @return 发送结果
     */
    public SendResult sendOrderly(String key, Object messageObject) {
        return sendOrderly(topic, key, messageObject, "");
    }

    /**
     * 顺序发消息,默认发送到第一个queue
     *
     * @param key           key
     * @param messageObject 消息对象
     * @param tag           标签
     * @return 发送结果
     */
    public SendResult sendOrderly(String key, Object messageObject, String tag) {
        return sendOrderly(topic, key, messageObject, tag);
    }


    /**
     * 顺序发消息,默认发送到第一个queue
     *
     * @param topic         topic
     * @param key           key
     * @param messageObject 消息对象
     * @param tag           标签
     * @return 发送结果
     */
    public SendResult sendOrderly(String topic, String key, Object messageObject, String tag) {
        byte[] msgBytes = JsonUtils.obj2Bytes(messageObject);

        Message msg = new Message(topic,
                tag,
                key,
                msgBytes
        );
        SendResult sendResult = null;
        try {
            sendResult = send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            }, topic);
        } catch (Exception e) {
            log.error("TOPIC={},KEY={},TAG={}", topic, key, tag, e);
        }
        return sendResult;
    }


    /**
     * 顺序发消息,默认发送到第一个queue
     *
     * @param key           key
     * @param messageObject 消息对象
     * @param tag           标签
     */
    public void sendAsyncOrderly(String key, Object messageObject, String tag) {
        sendAsyncOrderly(topic, key, messageObject, tag);
    }

    /**
     * 顺序发消息,默认发送到第一个queue
     *
     * @param topic         topic
     * @param key           key
     * @param messageObject 消息对象
     * @param tag           标签
     */
    public void sendAsyncOrderly(String topic, String key, Object messageObject, String tag) {
        byte[] msgBytes = JsonUtils.obj2Bytes(messageObject);

        Message msg = new Message(topic,
                tag,
                key,
                msgBytes
        );
        try {
            send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            }, topic, createCallback(messageObject));
        } catch (Exception e) {
            log.error("TOPIC={},KEY={},TAG={}", topic, key, tag, e);
        }
    }


    /**
     * 批量发送消息,允许有丢失
     *
     * @param messages 消息集合
     */
    public void batchSend(List<Message> messages) {
        ListSplitter splitter = new ListSplitter(messages);
        while (splitter.hasNext()) {
            try {
                List<Message> listItem = splitter.next();
                send(listItem);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
