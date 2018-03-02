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

package org.apache.rocketmq.mysql.position;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.rocketmq.mysql.Config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hexun.zookeeper.RegistryCenter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BinlogPositionManager {

	private RegistryCenter registryCenter;
	private DataSource dataSource;
	private Config config;

	private String binlogFilename;
	private Long nextPosition;

	public BinlogPositionManager(RegistryCenter registryCenter, Config config, DataSource dataSource) {
		this.registryCenter = registryCenter;
		this.config = config;
		this.dataSource = dataSource;
	}

	public void initBeginPosition() throws Exception {

		if (config.getStartType() == null || config.getStartType().equals("DEFAULT")) {
			initPositionDefault();

		} else if (config.getStartType().equals("NEW_EVENT")) {
			initPositionFromBinlogTail();

		} else if (config.getStartType().equals("LAST_PROCESSED")) {
			initPositionFromMqTail();

		} else if (config.getStartType().equals("SPECIFIED")) {
			binlogFilename = config.getBinlogFilename();
			nextPosition = config.getNextPosition();

		}

		if (binlogFilename == null || nextPosition == null) {
			throw new RuntimeException("binlogFilename | nextPosition is null.");
		}
	}

	private void initPositionDefault() throws Exception {

		try {
			initPositionFromMqTail();
		} catch (Exception e) {
			log.error("Init position from mq error.", e);
		}

		if (binlogFilename == null || nextPosition == null) {
			initPositionFromBinlogTail();
		}

	}

	/**
	 * 重写此方法 从 zk中获取 binlogFilename 和 nextPosition
	 * 
	 * @throws Exception
	 */
	private void initPositionFromMqTail() throws Exception {
		String json = registryCenter.get("/binlogPosition");
		JSONObject js = JSON.parseObject(json);
		binlogFilename = js.getString("binlogFilename");
		nextPosition = js.getLong("nextPosition");
	}

	private void initPositionFromBinlogTail() throws SQLException {
		String sql = "SHOW MASTER STATUS";

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();

			while (rs.next()) {
				binlogFilename = rs.getString("File");
				nextPosition = rs.getLong("Position");
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}

	public String getBinlogFilename() {
		return binlogFilename;
	}

	public Long getPosition() {
		return nextPosition;
	}
}
