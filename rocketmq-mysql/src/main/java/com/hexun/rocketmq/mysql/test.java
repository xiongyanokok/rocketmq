package com.hexun.rocketmq.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;

import java.io.IOException;

/**
 * Created by yuanyue on 2017/7/15.
 */
public class test {
    public static void main(String[] args) throws IOException {
        BinaryLogClient client = new BinaryLogClient("10.0.150.159", 3306, "peixun_test", "HexunXMpeixun");
        client.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {
                System.out.println(event);
            }
        });
        client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient binaryLogClient) {
                System.out.println(binaryLogClient);
            }

            @Override
            public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {
                System.out.println(binaryLogClient);

            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {
                System.out.println(binaryLogClient);

            }

            @Override
            public void onDisconnect(BinaryLogClient binaryLogClient) {
                System.out.println(binaryLogClient);

            }
        });
        client.connect();
    }
}
