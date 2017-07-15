package com.hexun.rocketmq.impl;

import com.hexun.rocketmq.MessageProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/*.xml"})
public class MessageProducerTest {

    @Autowired
    MessageProducer producer;

    @Test
    public void testAddAppointment() throws Exception {
        for (int i = 0; i < 1000; i++) {
            System.out.println(i);
            producer.sendAsync(i + "key", i + "msg", "");
        }
        Thread.sleep(2000000);
    }
}
