package com.hexun.rocketmq.client.utils;


import com.hexun.common.utils.StringUtils;
import com.hexun.rocketmq.client.BaseMessageConsumer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;


/**
 * 监控类 实现BeanPostProcessor接口
 */
@Component
public class MessageConsumerMap implements BeanPostProcessor {

    /**
     * 存放topic / tag / MessageListenerTemplate
     */
    static ConcurrentHashMap<String, ConcurrentHashMap<String, BaseMessageConsumer>> messageConsumerMap = new ConcurrentHashMap<>();


    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    /**
     * 在具体子类初始化之后 确认该对象是否是对应父类（BaseMessageConsumer）的子类
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // 如实现BaseMessageListener 则就执行put功能
        if (bean instanceof BaseMessageConsumer) {
            BaseMessageConsumer listener = (BaseMessageConsumer) bean;
            if (StringUtils.isNotBlank(listener.getTopic())) {
                ConcurrentHashMap<String, BaseMessageConsumer> tagMap = messageConsumerMap.get(listener.getTopic());
                if (tagMap == null) {
                    tagMap = new ConcurrentHashMap<>();
                    messageConsumerMap.put(listener.getTopic(), tagMap);
                }
                if (StringUtils.isNotBlank(listener.getTag())) {
                    tagMap.put(listener.getTag(), listener);
                }
            }
        }
        return bean;
    }

    /**
     * 根据topic获取listener
     *
     * @param topic topic
     * @return ConcurrentHashMap
     */
    public static ConcurrentHashMap<String, BaseMessageConsumer> getListenerByTopic(String topic) {
        return messageConsumerMap.get(topic);
    }

}