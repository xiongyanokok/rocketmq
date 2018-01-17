package com.hexun.rocketmq.client.test;

import com.hexun.common.utils.DateUtils;
import com.hexun.rocketmq.client.MessageProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/applicationContext-producer.xml"})
public class MessageProducerTest {

    @Autowired
    MessageProducer producer;

    @Test
    public void testAddAppointment() throws Exception {
        for (int i = 0; i < 100; i++) {
            System.out.println(i);
            String now = DateUtils.now();
            producer.sendAsync(i + "************" + now, "AAAAAAAAAAAA", "taga");
        }
        for (int i = 0; i < 100; i++) {
            System.out.println(i);
            String now = DateUtils.now();
            producer.sendAsync(i + "************" + now, "BBBBBBBBBBBB", "tagb");
        }
        Thread.sleep(2000000);
    }
}
