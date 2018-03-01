package com.hexun.rocketmq.client;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * tag 配置
 * 默认的group 是 CG-{TOPIC}-{TAG}
 *
 * @author yuanyue 87439247@qq.com
 */
public class TagsConfig extends TopicConfig {

    /**
     * 是否是集群消费
     */
    private boolean consumeCluster = true;

    /**
     * 是否是集群消费
     *
     * @param consumeCluster
     */
    public void setConsumeCluster(boolean consumeCluster) {
        this.consumeCluster = consumeCluster;
    }

    /**
     * logger
     */
    Logger logger = LoggerFactory.getLogger(TagsConfig.class);

    /**
     * 消费者
     */
    private List<MessageConsumer> consumers = new ArrayList<>();

    /**
     * tags
     */
    private List<Tag> tags = new ArrayList<>();

    /**
     * 设置tag和listener的键值对
     *
     * @param tagListenerMap Map<String, String>
     */
    public void setTagListenerMap(Map<String, String> tagListenerMap) {
        if (tagListenerMap != null && !tagListenerMap.isEmpty()) {
            for (Map.Entry<String, String> entry : tagListenerMap.entrySet()) {
                Tag tag = new Tag();
                tag.setTag(entry.getKey());
                tag.setListenerClass(entry.getValue());
                tags.add(tag);
            }
        }
    }

    @Override
    public void init() {
        super.init();
        for (Tag tag : tags) {
            try {
                MessageConsumer consumer = new MessageConsumer();
                consumers.add(consumer);
                consumer.setNamesrvAddr(getNamesrvAddr());
                consumer.setTopic(getTopic());
                consumer.setConsumeCluster(consumeCluster);
                consumer.setSubExpression(tag.getTag());
                consumer.setListenerClass(tag.listenerClass);
                consumer.setVipChannelEnabled(isVipChannelEnabled());
                String consumeGroup = "CG-" + getTopic();
                if (!BaseMessageConsumer.TAG_ALL.equals(tag.getTag())) {
                    consumeGroup = consumeGroup + "-" + tag.getTag();
                }
                consumer.setConsumerGroup(consumeGroup);
                consumer.init();
            } catch (MQClientException e) {
                logger.error("初始化失败topic={},tag={},listener={}", getTopic(), tag.getTag(), tag.getListenerClass(), e);
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                for (MessageConsumer consumer : consumers) {
                    try {
                        consumer.shutdown();
                    } catch (Exception e) {
                    }
                }
            }
        }));

    }


    /**
     * tag 和 listener
     */
    public class Tag {
        private String tag;
        private String listenerClass;

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public String getListenerClass() {
            return listenerClass;
        }

        public void setListenerClass(String listenerClass) {
            this.listenerClass = listenerClass;
        }
    }
}
