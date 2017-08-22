package com.hexun.rocketmq.web;


public interface WebMqService {
    Result postMsg(String topic, String tag, String key, String body);
}
