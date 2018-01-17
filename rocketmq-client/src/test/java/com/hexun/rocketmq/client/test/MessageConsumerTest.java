package com.hexun.rocketmq.client.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/applicationContext-consumer.xml"})
public class MessageConsumerTest {



    @Test
    public void testConsumer() throws Exception {
        Thread.sleep(5000000);
    }
}
