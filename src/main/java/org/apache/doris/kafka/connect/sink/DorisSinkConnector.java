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
package org.apache.doris.kafka.connect.sink;

import com.google.common.collect.Lists;
import org.apache.doris.kafka.connect.config.DorisSinkConfig;
import org.apache.doris.kafka.connect.utils.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * kafka connect doris sink connector
 */
public class DorisSinkConnector extends SinkConnector {

    private Map<String, String> config;

    public void start(Map<String, String> config) {
        this.config = config;
    }

    public Class<? extends Task> taskClass() {
        return DorisSinkTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = Lists.newArrayList();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(config));
        }
        return configs;
    }

    public void stop() {
        // do nothing
    }

    public ConfigDef config() {
        return DorisSinkConfig.conf;
    }

    public String version() {
        return VersionUtil.getVersion();
    }
}
