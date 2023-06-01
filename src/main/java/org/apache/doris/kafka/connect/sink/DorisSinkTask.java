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

import org.apache.doris.kafka.connect.config.DorisSinkConfig;
import org.apache.doris.kafka.connect.utils.VersionUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * kafka connect doris sink task
 */
public class DorisSinkTask extends SinkTask {
    private DorisSinkConfig config;

    private BufferRecords bufferRecords;
    public void start(Map<String, String> props) {
        this.config = new DorisSinkConfig(props);
        this.bufferRecords = new BufferRecords(this.config);
    }

    public void put(Collection<SinkRecord> records) {


            try {
                for (SinkRecord record : records) {
                    bufferRecords.add(record);
                }
                bufferRecords.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

    }

    public void stop() {
        // No-op
    }

    public String version() {
        return VersionUtil.getVersion();
    }
}
