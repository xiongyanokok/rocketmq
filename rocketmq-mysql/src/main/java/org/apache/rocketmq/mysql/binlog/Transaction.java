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
import java.util.Map;

import org.apache.rocketmq.mysql.position.BinlogPosition;
import org.apache.rocketmq.mysql.schema.Table;

import com.alibaba.fastjson.JSONObject;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Transaction {

	private BinlogPosition nextBinlogPosition;
	private Long xid;
	private DataRow dataRow;

	public void addRow(String type, Table table, Serializable[] before, Serializable[] row) {
		dataRow = new DataRow(type, table, before, row);
	}

	public String toJson() {
		Map<String, Object> map = new HashMap<>();
		map.put("xid", xid);
		map.put("binlogFilename", nextBinlogPosition.getBinlogFilename());
		map.put("nextPosition", nextBinlogPosition.getPosition());
		map.put("rows", dataRow.toMap());

		return JSONObject.toJSONString(map);
	}

}