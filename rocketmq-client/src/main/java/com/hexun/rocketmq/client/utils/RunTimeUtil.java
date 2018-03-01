package com.hexun.rocketmq.client.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class RunTimeUtil {
    /**
     * 取线程ID
     */
    private static int getPid() {
        String info = getRunTimeInfo();
        int pid = new Random().nextInt();
        int index = info.indexOf("@");
        if (index > 0) {
            pid = Integer.parseInt(info.substring(0, index));
        }
        return pid;
    }

    private static String getRunTimeInfo() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String info = runtime.getName();
        return info;
    }

    private static AtomicInteger index = new AtomicInteger();

    public static String getRocketMqUniqeInstanceName() {
        return "pid" + getPid() + "_index" + index.incrementAndGet();
    }
}
