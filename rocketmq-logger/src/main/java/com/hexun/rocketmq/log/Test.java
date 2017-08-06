package com.hexun.rocketmq.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yuanyue on 2017/7/29.
 */
public class Test {
    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(Test.class);
        for (int i = 5000; i < 5200; i++) {
            logger.error("测试文本" + i);
            if (i % 200 == 0)
                Thread.sleep(2000);

        }
        Thread.sleep(20000);
    }
}
