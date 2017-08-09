package com.hexun.rocketmq.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.status.ErrorStatus;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hexun.common.utils.IpUtils;
import com.hexun.common.utils.JsonUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.Charset;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yuanyue
 */
public class Appender extends AppenderBase<ILoggingEvent> {

    /**
     * 发送成功的计数器
     */
    AtomicLong successCount = new AtomicLong(0);

    /**
     * 发送失败的计数器
     */
    AtomicLong failCount = new AtomicLong(0);

    /**
     * message
     */
    BlockingQueue<Message> messagesQueue = new LinkedBlockingQueue<>(100000);

    /**
     * Message tag define
     */
    private String tag;

    /**
     * Whitch topic to send log messages
     */
    private String topic;

    /**
     * RocketMQ nameserver address
     */
    private String nameServerAddress;

    /**
     * Log producer send instance
     */
    private DefaultMQProducer producer;

    /**
     * layout
     */
    private Layout layout;

    Charset charset = Charset.forName("UTF-8");

    /**
     * Info,error,warn,callback method implementation
     *
     * @param event
     */
    @Override
    protected void append(ILoggingEvent event) {
        if (!isStarted()) {
            return;
        }
        String logStr = this.layout.doLayout(event);

        try {
            ObjectNode objectNode = (ObjectNode) JsonUtils.string2JsonNode(logStr);
            objectNode.put("sip", IpUtils.getHostIP());
            objectNode.put("host", IpUtils.getHostName());
            objectNode.put("tag", tag);
            objectNode.put("topic", topic);
            Message msg = new Message(topic, tag, objectNode.toString().getBytes(charset));
            messagesQueue.add(msg);
        } catch (Exception e) {
            addError("Could not send message in RocketmqLogbackAppender [" + name + "]. Message is : " + logStr, e);
        }
    }

    /**
     * Options are activated and become effective only after calling this method.
     */
    public void start() {
        int errors = 0;

        if (this.layout == null) {
            addStatus(new ErrorStatus("No layout set for the RocketmqLogbackAppender named \"" + name + "\".", this));
            errors++;
        }

        if (errors > 0 || !checkEntryConditions()) {
            return;
        }
        try {
            String producerGroup = "PG-" + topic;
            producer = new DefaultMQProducer();
            producer.setNamesrvAddr(nameServerAddress);
            producer.setVipChannelEnabled(false);
            producer.setProducerGroup(producerGroup);
            producer.setSendMsgTimeout(20000);
            producer.setRetryTimesWhenSendFailed(3);
            producer.setCreateTopicKey(topic);
            producer.start();
            started = true;
        } catch (Exception e) {
            addError("Starting RocketmqLogbackAppender [" + this.getName()
                    + "] nameServerAddress:" + nameServerAddress + " topic:" + topic + " " + e.getMessage());
        }
        if (started) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Message message = messagesQueue.take();
                            if (message != null) {
                                SendResult sendResult = producer.send(message);
                                long l = SendStatus.SEND_OK.equals(sendResult.getSendStatus()) ?
                                        successCount.incrementAndGet() :
                                        failCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            failCount.incrementAndGet();
                            addError("while take msg error", e);
                        }
                    }
                }
            }).start();
            Timer timer = new Timer("print send result stat");
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    addInfo("ROCKETMQ日志发送状态:成功" + successCount + ",失败" + failCount);
                }
            }, 0, 10000);
        }
        if (producer != null) {
            super.start();
        }
    }

    /**
     * When system exit,this method will be called to close resources
     */
    public synchronized void stop() {
        this.started = false;

        try {
            producer.shutdown();
        } catch (Exception e) {
            addError("Closeing RocketmqLogbackAppender [" + this.getName()
                    + "] nameServerAddress:" + nameServerAddress + " topic:" + topic, e);
        }

        // Help garbage collection
        producer = null;
    }


    boolean checkEntryConditions() {
        if (this.topic == null) {
            addError("No topic for RocketmqLogbackAppender named [" + name + "].");
            return false;
        }
        return true;
    }

    /**
     * Set the pattern layout to format the log.
     */
    public void setLayout(Layout layout) {
        this.layout = layout;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setNameServerAddress(String nameServerAddress) {
        this.nameServerAddress = nameServerAddress;
    }
}
