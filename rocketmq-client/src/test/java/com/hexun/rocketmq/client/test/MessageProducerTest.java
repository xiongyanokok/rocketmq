package com.hexun.rocketmq.client.test;

import com.hexun.rocketmq.client.MessageProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/applicationContext-producer.xml"})
public class MessageProducerTest {


    @Autowired
    MessageProducer producer;

    @Test
    public void testP() throws Exception {
        for (int i = 0; i < 5; i++) {
            producer.send("yuanyue", "aaa", "aaa");
        }
    }
}
