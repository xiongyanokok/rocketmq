//package com.hexun.rocketmq.client.extension.impl;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//
//import com.hexun.rocketmq.client.extension.MessageConsumer;
//
//public class MessagePushConsumerImplTest {
//	public static void main(String[] args) throws Exception {
//		MessagePushConsumerImpl mpc = new MessagePushConsumerImpl();
//		mpc.setSerializeType("fastJson");
//		mpc.setTopic("topicTest1");
//		mpc.setSubExpression("*");
//		mpc.setGroup("rocketmq");
//		mpc.setDataId("url");
//		mpc.setMessageConsumers(new ArrayList<MessageConsumer<Serializable>>() {
//			{
//				add(new MessageConsumer<Serializable>() {
//
//					@Override
//					public void consume(Serializable messageObject) throws Exception {
//						System.out.println(messageObject);
//					}
//				});
//			}
//		});
//
//		mpc.afterPropertiesSet();
//
//		Thread.sleep(100000L);
//		mpc.destroy();
//	}
//}