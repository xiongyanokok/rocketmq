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

package org.apache.rocketmq.mysql.binlog;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.rocketmq.mysql.Config;
import org.apache.rocketmq.mysql.Replicator;
import org.apache.rocketmq.mysql.position.BinlogPosition;
import org.apache.rocketmq.mysql.position.BinlogPositionManager;
import org.apache.rocketmq.mysql.schema.Schema;
import org.apache.rocketmq.mysql.schema.Table;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventProcessor {

	private Replicator replicator;

	private Config config;

	private DataSource dataSource;

	private BinaryLogClient binaryLogClient;

	private Schema schema;

	private Map<Long, Table> tableMap = new HashMap<>();

	private Transaction transaction;

	public EventProcessor(Replicator replicator) {
		this.replicator = replicator;
		this.config = replicator.getConfig();
		this.transaction = new Transaction();
	}

	public void start() throws Exception {
		// 初始化数据库
		initDataSource();

		// 初始化mysql binlog 开始位置
		BinlogPositionManager binlogPositionManager = new BinlogPositionManager(replicator.getRegistryCenter(), config, dataSource);
		binlogPositionManager.initBeginPosition();

		// 初始化Schema
		schema = new Schema(dataSource);
		schema.load();

		// BinaryLogClient
		binaryLogClient = new BinaryLogClient(config.getMysqlAddr(), config.getMysqlPort(), config.getMysqlUsername(), config.getMysqlPassword());
		binaryLogClient.setBlocking(true);
		binaryLogClient.setServerId(1001);

		EventDeserializer eventDeserializer = new EventDeserializer();
		eventDeserializer.setCompatibilityMode(CompatibilityMode.DATE_AND_TIME_AS_LONG, CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
		binaryLogClient.setEventDeserializer(eventDeserializer);
		binaryLogClient.registerEventListener(new EventListener(this));
		binaryLogClient.setBinlogFilename(binlogPositionManager.getBinlogFilename());
		binaryLogClient.setBinlogPosition(binlogPositionManager.getPosition());

		binaryLogClient.connect(3000);

		log.info("Started.");
	}

	public void doProcess(Event event) {
		try {
			// EventType
			EventType eventType = event.getHeader().getEventType();
			switch (eventType) {
				case TABLE_MAP:
					processTableMapEvent(event);
					break;
	
				case WRITE_ROWS:
				case EXT_WRITE_ROWS:
					processInsertEvent(event);
					break;
	
				case UPDATE_ROWS:
				case EXT_UPDATE_ROWS:
					processUpdateEvent(event);
					break;
	
				case DELETE_ROWS:
				case EXT_DELETE_ROWS:
					processDeleteEvent(event);
					break;
	
				case QUERY:
					processQueryEvent(event);
					break;
	
				case XID:
					processXidEvent(event);
					break;
	
				default:
					break;
			}
		} catch (Exception e) {
			log.error("Binlog process error.", e);
		}
	}

	private void checkConnection() throws IOException, TimeoutException {
		if (!binaryLogClient.isConnected()) {
			BinlogPosition binlogPosition = replicator.getNextBinlogPosition();
			if (binlogPosition != null) {
				binaryLogClient.setBinlogFilename(binlogPosition.getBinlogFilename());
				binaryLogClient.setBinlogPosition(binlogPosition.getPosition());
			}

			binaryLogClient.connect(3000);
		}
	}

	private void processTableMapEvent(Event event) {
		TableMapEventData data = event.getData();
		String dbName = data.getDatabase();
		String tableName = data.getTable();

		// 过滤 库 表
		if (!(dbName.equals("cdsq") && tableName.equals("cdsq_article"))) {
			log.info("dbName={}，tableName={}", dbName, tableName);
			return;
		}

		Table table = schema.getTable(dbName, tableName);
		tableMap.put(data.getTableId(), table);
	}

	private void processInsertEvent(Event event) {
		WriteRowsEventData data = event.getData();
		Long tableId = data.getTableId();
		List<Serializable[]> list = data.getRows();

		for (Serializable[] row : list) {
			addRow("INSERT", tableId, null, row);
		}
	}

	private void processUpdateEvent(Event event) {
		UpdateRowsEventData data = event.getData();
		Long tableId = data.getTableId();
		List<Map.Entry<Serializable[], Serializable[]>> list = data.getRows();

		for (Map.Entry<Serializable[], Serializable[]> entry : list) {
			addRow("UPDATE", tableId, entry.getKey(), entry.getValue());
		}
	}

	private void processDeleteEvent(Event event) {
		DeleteRowsEventData data = event.getData();
		Long tableId = data.getTableId();
		List<Serializable[]> list = data.getRows();

		for (Serializable[] row : list) {
			addRow("DELETE", tableId, null, row);
		}

	}

	private static Pattern createTablePattern = Pattern.compile("^(CREATE|ALTER)\\s+TABLE", Pattern.CASE_INSENSITIVE);

	private void processQueryEvent(Event event) {
		QueryEventData data = event.getData();
		String sql = data.getSql();

		if (createTablePattern.matcher(sql).find()) {
			schema.reset();
		}
	}

	private void processXidEvent(Event event) {
		EventHeaderV4 header = event.getHeader();
		XidEventData data = event.getData();

		BinlogPosition binlogPosition = new BinlogPosition(binaryLogClient.getBinlogFilename(), header.getNextPosition());
		transaction.setNextBinlogPosition(binlogPosition);
		transaction.setXid(data.getXid());

		replicator.commit(transaction);

		transaction = new Transaction();
	}

	private void addRow(String type, Long tableId, Serializable[] before, Serializable[] row) {
		Table t = tableMap.get(tableId);
		if (t != null) {
			transaction.addRow(type, t, before, row);
		}
	}

	private void initDataSource() throws Exception {
		Map<String, String> map = new HashMap<>();
		map.put("driverClassName", "com.mysql.jdbc.Driver");
		map.put("url", "jdbc:mysql://" + config.getMysqlAddr() + ":" + config.getMysqlPort() + "?useSSL=true&verifyServerCertificate=false");
		map.put("username", config.getMysqlUsername());
		map.put("password", config.getMysqlPassword());
		map.put("initialSize", "2");
		map.put("maxActive", "2");
		map.put("maxWait", "60000");
		map.put("timeBetweenEvictionRunsMillis", "60000");
		map.put("minEvictableIdleTimeMillis", "300000");
		map.put("validationQuery", "SELECT 1 FROM DUAL");
		map.put("testWhileIdle", "true");

		dataSource = DruidDataSourceFactory.createDataSource(map);
	}

}
