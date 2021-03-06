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

package org.apache.rocketmq.mysql.schema;

import java.util.LinkedList;
import java.util.List;

import org.apache.rocketmq.mysql.schema.column.ColumnParser;

import lombok.Getter;

@Getter
public class Table {
	
	private String database;
	private String tableName;
	private List<String> colList = new LinkedList<>();
	private List<ColumnParser> parserList = new LinkedList<>();

	public Table(String database, String tableName) {
		this.database = database;
		this.tableName = tableName;
	}

	public void addCol(String column) {
		colList.add(column);
	}

	public void addParser(ColumnParser columnParser) {
		parserList.add(columnParser);
	}

}