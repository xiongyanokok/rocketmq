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
package org.apache.rocketmq.mysql;

import org.apache.rocketmq.mysql.binlog.EventProcessor;
import org.apache.rocketmq.mysql.binlog.Transaction;
import org.apache.rocketmq.mysql.position.BinlogPosition;
import org.apache.rocketmq.mysql.position.BinlogPositionLogThread;
import org.apache.rocketmq.mysql.productor.RocketMQProducer;

import com.hexun.zookeeper.RegistryCenter;
import com.hexun.zookeeper.ZookeeperRegistryCenter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Replicator {
	
	private RegistryCenter registryCenter;

	private Config config;

	private RocketMQProducer rocketMQProducer;

	private Object lock = new Object();

	private BinlogPosition nextBinlogPosition;
	private long xid;

	public static void main(String[] args) {
		Replicator replicator = new Replicator();
		replicator.start();
	}
	
	public void start() {
		try {
			// 加载配置文件
			config = new Config();
			config.load();
			
			// 加载zookeeper
			ZookeeperRegistryCenter zookeeperRegistryCenter = new ZookeeperRegistryCenter();
			zookeeperRegistryCenter.setServers(config.getZkAddr());
			zookeeperRegistryCenter.setNamespace(config.getZkNamespace());
			zookeeperRegistryCenter.init();
			registryCenter = zookeeperRegistryCenter;
			
			// 加锁（分布式环境防止重复消费binlog日志）
			lock();

			// 连接RocketMQ
			rocketMQProducer = new RocketMQProducer(config);
			rocketMQProducer.start();

			// 每秒钟记录最近一次二进制日志位置和偏移量
			BinlogPositionLogThread binlogPositionLogThread = new BinlogPositionLogThread(this);
			binlogPositionLogThread.start();

			// 事件处理
			EventProcessor eventProcessor = new EventProcessor(this);
			eventProcessor.start();
			
		} catch (Exception e) {
			log.error("Start error.", e);
			System.exit(1);
		}
	}
	
	private void lock() {
		if (registryCenter.isExisted("/lock")) {
			try {
				Thread.sleep(10000);
			} catch (Exception e) {
				// 
			}
			lock();
		} else {
			registryCenter.ephemeral("/lock", "lock");
		}
	}

	public void commit(Transaction transaction) {
		for (int i = 0; i < 3; i++) {
			try {
				if (null != transaction.getDataRow()) {
					rocketMQProducer.push(transaction.toJson());
				}
				break;
			} catch (Exception e) {
				log.error("Push error， retry:{} times", (i + 1), e);
			} finally {
				synchronized (lock) {
					xid = transaction.getXid();
					nextBinlogPosition = transaction.getNextBinlogPosition();
				}
			}
		}
	}

	public void logPosition() {
		if (nextBinlogPosition != null) {
			synchronized (lock) {
				// 二进制日志文件
				String binlogFilename = nextBinlogPosition.getBinlogFilename();
				// 偏移量
				long nextPosition = nextBinlogPosition.getPosition();
				log.info("XID: {}，BINLOG_FILE: {}，NEXT_POSITION: {}", xid, binlogFilename, nextPosition);

				// 持久化数据到zk中
				registryCenter.persist("/binlogPosition", "{\"binlogFilename\":\"" + binlogFilename + "\", \"nextPosition\":" + nextPosition + "}");
			}
		}
	}

	public Config getConfig() {
		return config;
	}

	public BinlogPosition getNextBinlogPosition() {
		return nextBinlogPosition;
	}
	
	public RegistryCenter getRegistryCenter() {
		return registryCenter;
	}

}
