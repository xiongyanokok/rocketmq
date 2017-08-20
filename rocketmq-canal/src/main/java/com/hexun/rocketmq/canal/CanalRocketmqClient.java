package com.hexun.rocketmq.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * CanalPrinter
 */
class CanalRocketmqClient {

    /**
     * logger
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * 标识.控制流程
     */
    private volatile boolean running = false;
    /**
     * 线程异常处理
     */
    private Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };
    /**
     * 后台线程
     */
    private Thread thread = null;
    /**
     *
     */
    private CanalConnector connector;

    /**
     * canal 消费者, rocketmq 生产者
     */
    private RocketmqProducer rocketmqProducer;

    /**
     * 构造器
     * @param connector CanalConnector
     * @param rocketmqProducer RocketmqProducer
     */
    CanalRocketmqClient(CanalConnector connector, RocketmqProducer rocketmqProducer) {
        this.connector = connector;
        this.rocketmqProducer = rocketmqProducer;
    }

    void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {
            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void process() {
        int batchSize = 1;
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    //发送到 rocketmq
                    boolean success = rocketmqProducer.sendToMq(message);
                    if (success) {
                        // 提交确认
                        connector.ack(batchId);
                    } else {
                        // 处理失败, 回滚数据
                        connector.rollback(batchId);
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
            }
        }
    }
}
