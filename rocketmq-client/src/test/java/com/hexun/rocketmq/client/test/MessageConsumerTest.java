package com.hexun.rocketmq.client.test;

import com.hexun.rocketmq.client.MessageProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/applicationContext-consumer.xml"})
public class MessageConsumerTest {


    @Autowired
    MessageProducer producer;

    @Test
    public void testConsumer() throws Exception {
        Thread.sleep(5000000);
    }
}
