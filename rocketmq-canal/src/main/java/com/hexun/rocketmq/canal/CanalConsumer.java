package com.hexun.rocketmq.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.hexun.rocketmq.client.MessageProducer;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalConsumer {

    private static Logger logger = LoggerFactory.getLogger(CanalConsumer.class);

    public static void main(String[] args) throws InterruptedException {
        String zkAddress = ConfigUtils.getString("zkAddress");
        String rocketmqAddress = ConfigUtils.getString("rocketmqAddress");
        String[] destinations = ConfigUtils.getString("destinations").split(",");
        for (String destination : destinations) {
            /*********ROCKETMQ INIT***********/
            //rocketmq init
            final MessageProducer messageProducer = new MessageProducer();
            messageProducer.setNamesrvAddr(rocketmqAddress);
            messageProducer.setTopic("BenchmarkTest");
            //canal rocketmq producer
            RocketmqProducer rocketmqProducer;
            try {
                rocketmqProducer = new RocketmqProducer(messageProducer);
            } catch (MQClientException e) {
                logger.error("init rocketmqProducer error", e);
                Thread.sleep(1000L);
                return;
            }
            /*********CANAL INIT***********/
            //canal init
            CanalConnector connector = CanalConnectors.newClusterConnector(zkAddress, destination, "", "");
            final CanalRocketmqClient canalRocketmqClient = new CanalRocketmqClient(connector, rocketmqProducer);
            canalRocketmqClient.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    logger.info("## stopping the canal & rocketmq  client");
                    try {
                        if (canalRocketmqClient != null)
                            canalRocketmqClient.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                    }
                    try {
                        if (messageProducer != null)
                            messageProducer.destroy();
                    } catch (Exception e) {
                        logger.warn("##something goes wrong when stopping rocketmq producer:\n{}", ExceptionUtils.getFullStackTrace(e));
                    }
                    logger.info("## canal & rocketmq client is stop.");
                }

            });
        }


    }
}
