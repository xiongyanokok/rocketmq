package com.hexun.rocketmq.client.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hexun.common.utils.JsonUtils;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 消息处理转换工具
 */
public class MessageUtil {

    /**
     * 消息对象转为bean ,只适用于发送的是json对象
     *
     * @param messageExt messageExt
     * @param classes    Class<T>
     * @param <T>        类型
     * @return T
     */
    public static <T> T msgToObj(MessageExt messageExt, Class<T> classes) {
        if (messageExt == null || messageExt.getBody() == null) {
            return null;
        }
        return JsonUtils.bytes2Obj(messageExt.getBody(), classes);
    }

    /**
     * 消息对象转为bean ,只适用于发送的是json对象
     *
     * @param messageExt    messageExt
     * @param typeReference TypeReference<T>
     * @param <T>           类型
     * @return T
     */
    public static <T> T msgToObj(MessageExt messageExt, TypeReference<T> typeReference) {
        if (messageExt == null || messageExt.getBody() == null) {
            return null;
        }
        return JsonUtils.bytes2Obj(messageExt.getBody(), typeReference);
    }
}
