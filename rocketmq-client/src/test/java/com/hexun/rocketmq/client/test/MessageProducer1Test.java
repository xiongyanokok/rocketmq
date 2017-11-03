package com.hexun.rocketmq.client.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.hexun.common.http.RequestPackage;
import com.hexun.common.http.ResponsePackage;
import com.hexun.common.security.Md5Utils;
import com.hexun.common.utils.DateUtils;
import com.hexun.common.utils.JsonUtils;
import com.hexun.common.utils.SignParameterUtils;
import com.hexun.rocketmq.client.MessageProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MessageProducer1Test {

    @Test
    public void httpRequest() throws InterruptedException {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("topic", "BLOG-TO-CAIDAO");
        data.put("tag", "18452236");
        data.put("key", new Date());
        data.put("body", "18452236" + new Date());

        String sign = Md5Utils.md5(SignParameterUtils.createSignText(data) + "Torocketmqapi");
        System.out.println("sign=" + sign);
        data.put("sign", sign);
        String url = "http://webmq.intcoop.hexun.com/mq/post";
        // 测试地址
        // String url = "http://10.4.63.104:9876/mq/post";
        ResponsePackage response = RequestPackage.post(url, data).setCharset("UTF-8").getResponse();
        JsonNode node = JsonUtils.string2JsonNode(response.getContent());
        if (null != node) {
            String msg = node.get("data").asText();
            System.out.println("data=" + msg);

        }
    }
}
