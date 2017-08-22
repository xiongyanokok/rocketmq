package com.hexun.rocketmq.client;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

class HealthChecker {
    /**
     * logger
     */
    Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * timer
     */
    Timer timer;
    /**
     * 错误次数
     */
    static AtomicInteger errorTimes = new AtomicInteger(0);

    /**
     * 健康检查
     *
     * @param producer MessageProducer
     */
    HealthChecker(final MessageProducer producer) {
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (errorTimes.get() >= 3) {
                    try {
                        producer.shutdown();
                        logger.error("producer 关闭");
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        logger.error("producer 正在启动");
                        producer.start();
                    } catch (MQClientException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(producer.getTopic());
                    if (messageQueues == null || messageQueues.isEmpty()) {
                        logger.error("健康检查:TOPIC={},队列为空", producer.getTopic());
                        errorTimes.addAndGet(1);
                    } else {
                        logger.error("健康检查:TOPIC={},OK", producer.getTopic());
                    }
                } catch (MQClientException e) {
                    e.printStackTrace();
                    logger.error("健康检查:TOPIC={},连接异常", producer.getTopic());
                    errorTimes.addAndGet(1);
                }
            }
        }, 5 * 60 * 1000, 30 * 1000);
    }
}
