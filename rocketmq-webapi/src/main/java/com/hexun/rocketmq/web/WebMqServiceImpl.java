package com.hexun.rocketmq.web;

import com.alibaba.dubbo.rpc.protocol.rest.support.ContentType;
import com.hexun.rocketmq.client.MessageProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

/**
 * web生产者
 * @author yuanyue@staff.hexun.com
 */
@Service
@Path("mq")
@Consumes({MediaType.WILDCARD, MediaType.TEXT_XML})
@Produces({ContentType.APPLICATION_JSON_UTF_8, ContentType.TEXT_XML_UTF_8})
public class WebMqServiceImpl implements WebMqService {
    /**
     * Logger
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 消息生产者
     */
    @Autowired
    MessageProducer messageProducer;

    @Path("post")
    @POST
    @Override
    public Result postMsg(@FormParam("topic") String topic,
                          @FormParam("tag") String tag,
                          @FormParam("key") String key,
                          @FormParam("body") String body) {
        try {
            SendResult sendResult = messageProducer.send(topic, key, body, tag);
            if (sendResult != null && SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                return Result.success(sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Result.fail(-1, "发送失败");
    }
}
