package com.hexun.rocketmq.client.test;

import com.hexun.common.utils.DateUtils;
import com.hexun.rocketmq.client.MessageProducer;
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
            String now = DateUtils.now();
            producer.sendAsync(i + now, i + now, now);
        }
        Thread.sleep(2000000);
    }
}
