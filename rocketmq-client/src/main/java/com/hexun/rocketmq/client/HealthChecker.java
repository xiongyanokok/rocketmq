package com.hexun.rocketmq.client;

import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 健康检查
 * 每30秒检测一次 Producer 状态
 */
class HealthChecker {
    /**
     * logger
     */
    private static Logger logger = LoggerFactory.getLogger(HealthChecker.class);
    /**
     * 错误次数
     */
    private static AtomicInteger errorTimes = new AtomicInteger(0);

    /**
     * 健康检查
     *
     * @param producer MessageProducer
     */
    public static void HealthChecker(final MessageProducer producer) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(producer.getTopic());
                    if (messageQueues == null || messageQueues.isEmpty()) {
                        errorTimes.addAndGet(1);
                        logger.error("健康检查:TOPIC={},队列为空,失败次数{}", producer.getTopic(), errorTimes.get());
                    } else {
                        logger.info("健康检查:TOPIC={},OK", producer.getTopic());
                    }
                } catch (Exception e) {
                    errorTimes.addAndGet(1);
                    logger.error("健康检查:TOPIC={},连接异常,失败次数{}", producer.getTopic(), errorTimes.get(), e);
                }
            }
        }, 5 * 60 * 1000, 30 * 1000);
    }
}
