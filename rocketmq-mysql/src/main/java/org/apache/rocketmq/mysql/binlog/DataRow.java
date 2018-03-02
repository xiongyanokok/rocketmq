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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.mysql.schema.Table;
import org.apache.rocketmq.mysql.schema.column.ColumnParser;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataRow {

	private String type;
	@Getter private Table table;
	private Serializable[] before;
	private Serializable[] row;

	public DataRow(String type, Table table, Serializable[] before, Serializable[] row) {
		this.type = type;
		this.table = table;
		this.before = before;
		this.row = row;
	}

	public Map<String, Object> toMap() {

		try {
			if (table.getColList().size() == row.length) {
				List<String> keyList = table.getColList();
				List<ColumnParser> parserList = table.getParserList();

				Map<String, Object> beforeDataMap = new HashMap<>();
				if (null != before) {
					for (int i = 0; i < keyList.size(); i++) {
						Object value = before[i];
						ColumnParser parser = parserList.get(i);
						beforeDataMap.put(keyList.get(i), parser.getValue(value));
					}
				}

				Map<String, Object> dataMap = new HashMap<>();
				for (int i = 0; i < keyList.size(); i++) {
					Object value = row[i];
					ColumnParser parser = parserList.get(i);
					dataMap.put(keyList.get(i), parser.getValue(value));
				}

				Map<String, Object> map = new HashMap<>();
				map.put("database", table.getDatabase());
				map.put("table", table.getTableName());
				map.put("type", type);
				map.put("beforeData", beforeDataMap);
				map.put("data", dataMap);
				return map;
			} else {
				log.error("Table schema changed，discard data: {} - {}, {}  {}  {}", table.getDatabase(), table.getTableName(), type, before, row);
			}
		} catch (Exception e) {
			log.error("Row parse error，discard data: {} - {}, {}  {}  {}", table.getDatabase(), table.getTableName(), type, before, row);
		}
		return null;
	}
}
