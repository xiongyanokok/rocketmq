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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Config {

	private String mysqlAddr;
	private Integer mysqlPort;
	private String mysqlUsername;
	private String mysqlPassword;

	private String mqNamesrvAddr;
	private String mqTopic;

	private String startType = "DEFAULT";
	private String binlogFilename;
	private Long nextPosition;
	private Integer maxTransactionRows = 1;
	
	private String zkAddr;
	private String zkNamespace;

	public void load() throws IOException {
		// 加载配置文件
		InputStream in = Config.class.getClassLoader().getResourceAsStream("rocketmq_mysql.conf");
		Properties properties = new Properties();
		properties.load(in);

		// 对象赋值
		properties2Object(properties, this);
	}

	private void properties2Object(final Properties p, final Object object) {
		Method[] methods = object.getClass().getMethods();
		for (Method method : methods) {
			String mn = method.getName();
			if (mn.startsWith("set")) {
				try {
					String tmp = mn.substring(4);
					String first = mn.substring(3, 4);

					String key = first.toLowerCase() + tmp;
					String property = p.getProperty(key);
					if (property != null) {
						Class<?>[] pt = method.getParameterTypes();
						if (pt != null && pt.length > 0) {
							String cn = pt[0].getSimpleName();
							Object arg;
							if (cn.equals("int") || cn.equals("Integer")) {
								arg = Integer.parseInt(property);
							} else if (cn.equals("long") || cn.equals("Long")) {
								arg = Long.parseLong(property);
							} else if (cn.equals("double") || cn.equals("Double")) {
								arg = Double.parseDouble(property);
							} else if (cn.equals("boolean") || cn.equals("Boolean")) {
								arg = Boolean.parseBoolean(property);
							} else if (cn.equals("float") || cn.equals("Float")) {
								arg = Float.parseFloat(property);
							} else if (cn.equals("String")) {
								arg = property;
							} else {
								continue;
							}
							method.invoke(object, arg);
						}
					}
				} catch (Exception e) {
					//
				}
			}
		}
	}

}