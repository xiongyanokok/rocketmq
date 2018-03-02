/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mysql.productor;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.mysql.Config;
import org.apache.rocketmq.mysql.binlog.Transaction;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RocketMQProducer {

	private DefaultMQProducer producer;
	private Config config;

	public RocketMQProducer(Config config) {
		this.config = config;
	}

	public void start() throws MQClientException {
		producer = new DefaultMQProducer("BINLOG_PRODUCER_GROUP");
		producer.setNamesrvAddr(config.getMqNamesrvAddr());
		producer.start();
	}

	public long push(Transaction transaction) throws Exception {
		String json = transaction.toJson();
		log.debug("--------------->{}", transaction.toJson());

		Message message = new Message(config.getMqTopic(), json.getBytes("UTF-8"));
		// 设置tag
		message.setTags(transaction.getDataRow().getTable().getTableName());
		// 设置key
		message.setKeys(transaction.getNextBinlogPosition().getPosition().toString());
		SendResult sendResult = producer.send(message);

		return sendResult.getQueueOffset();
	}
}