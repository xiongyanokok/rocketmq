package com.hexun.rocketmq.client;

import com.hexun.common.utils.StringUtils;
import com.hexun.rocketmq.client.utils.MessageConsumerMap;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.hexun.rocketmq.client.BaseMessageConsumer.TAG_ALL;

public class DefaultMessageListener implements MessageListenerConcurrently {

    /**
     * logger
     */
    Logger logger = LoggerFactory.getLogger(DefaultMessageListener.class);

    /**
     * 消费数据
     *
     * @param msgs    List<MessageExt>
     * @param context ConsumeConcurrentlyContext
     * @return ConsumeConcurrentlyStatus
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msgExt : msgs) {
                logger.info("consuming msg id={} key={}", msgExt.getMsgId(), msgExt.getKeys());
                //根据topic 获取tag消费者
                ConcurrentHashMap<String, BaseMessageConsumer> listenerByTopic = MessageConsumerMap.getListenerByTopic(msgExt.getTopic());
                if (listenerByTopic != null) {
                    BaseMessageConsumer messageConsumer = null;
                    if (StringUtils.isNotBlank(msgExt.getTags())) {
                        messageConsumer = listenerByTopic.get(msgExt.getTags());
                    }
                    if (messageConsumer == null) {
                        messageConsumer = listenerByTopic.get(TAG_ALL);
                    }
                    if (messageConsumer == null) {
                        continue;
                    }
                    ConsumeOrderlyStatus consumeOrderlyStatus = messageConsumer.consume(msgExt);
                    if (consumeOrderlyStatus != null && consumeOrderlyStatus == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT) {
                        logger.info("consuming msg id={} key={} consume failed", msgExt.getMsgId(), msgExt.getKeys());
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
