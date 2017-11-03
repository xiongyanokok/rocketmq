package com.hexun.rocketmq.web;

import com.alibaba.dubbo.rpc.protocol.rest.support.ContentType;
import com.hexun.common.utils.StringUtils;
import com.hexun.dubbo.anno.Signature;
import com.hexun.rocketmq.client.MessageProducer;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.nio.charset.Charset;

@Service
@Path("mq")
@Consumes({MediaType.WILDCARD, MediaType.TEXT_XML})
@Produces({ContentType.APPLICATION_JSON_UTF_8, ContentType.TEXT_XML_UTF_8})
public class WebMqServiceImpl implements WebMqService {

    Charset utf8 = Charset.forName("UTF-8");
    /**
     * Logger
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 消息生产者
     */
    @Autowired
    MessageProducer messageProducer;

    @Signature(signKey = "Torocketmqapi")
    @Path("post")
    @POST
    @Override
    public Result postMsg(@FormParam("topic") String topic, @FormParam("tag") String tag, @FormParam("key") String key, @FormParam("body") String body, @FormParam("sign") String sign) {
        try {
            logger.info("接收到的参数为topic={},tag={},key={},body={}", topic, tag, key, body);
            if (!StringUtils.isNoneBlank(topic) || !StringUtils.isNoneBlank(tag) || !StringUtils.isNoneBlank(key) || !StringUtils.isNoneBlank(body)) {
                return Result.fail(-2, "参数不能为空");
            }
            logger.info("开始插入队列");
            byte[] msgBody = body.getBytes(utf8);
            SendResult sendResult = messageProducer.send(topic, key, msgBody, tag);
            if (sendResult != null && SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                logger.info("插入队列成功sendResult={}", sendResult);
                return Result.success(sendResult.getSendStatus());
            }
        } catch (Exception e) {
            logger.error("插入队列异常", e);
        }
        logger.info("插入队列失败 topic={},tag={},key={},body={}", topic, tag, key, body);
        return Result.fail(-1, "发送失败");
    }
}
