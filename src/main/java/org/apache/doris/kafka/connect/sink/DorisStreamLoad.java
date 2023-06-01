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

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.kafka.connect.common.TableId;
import org.apache.doris.kafka.connect.config.DorisSinkConfig;
import org.apache.doris.kafka.connect.rest.HttpPutBuilder;
import org.apache.doris.kafka.connect.utils.HttpUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class DorisStreamLoad {

    private static final String LOAD_URL_FORMAT = "http://%s/api/%s/%s/_stream_load";
    private static final String ABORT_URL_FORMAT = "http://%s/api/%s/_stream_load_2pc";
    private static final String COLUMN_SEPARATOR = "column_separator";
    private static final String FORMAT = "format";
    private static final String LABEL = "label";

    private final DorisSinkConfig config;
    private final String loadUrlStr;

    private final String abortUrlStr;

    public DorisStreamLoad(DorisSinkConfig config) {
        this.config = config;
        TableId tableId = parseTableIdentifier();
        this.loadUrlStr = String.format(LOAD_URL_FORMAT, config.getHost(), tableId.getDatabase(), tableId.getTable());
        this.abortUrlStr = String.format(ABORT_URL_FORMAT, config.getHost(), tableId.getDatabase());
    }

    /**
     * Write record
     *
     * @param data
     * @param label
     * @throws IOException
     */
    public void writeRecord(String data, String label) throws IOException {
        try (CloseableHttpClient client = HttpUtil.getHttpClient()) {
            Properties props = new Properties();
            props.put(COLUMN_SEPARATOR, config.getColumnSeparator());
            props.put(FORMAT, config.getFormat().name().toLowerCase());
            props.put(LABEL, label);
            HttpPutBuilder putBuilder = new HttpPutBuilder()
                .setUrl(loadUrlStr)
                .baseAuth(config.getUsername(), config.getPassword())
                .addCommonHeader()
                .addProperties(props);
            putBuilder.setEntity(new StringEntity(data));
            if (config.getEnable2PC()) {
                putBuilder.enable2PC();
            }
            CloseableHttpResponse response = client.execute(putBuilder.build());
            final int statusCode = response.getStatusLine().getStatusCode();
            if (response.getEntity() != null) {
                String result = EntityUtils.toString(response.getEntity());
                log.info("Load stream result: " + result);
                if (statusCode != 200) {
                    throw new IOException(String.format("Stream load failed. status: %s load result: %s", statusCode, result));
                }
            }
        }
    }

    private TableId parseTableIdentifier() {
        String[] tableIdentifier = config.getTableIdentifier().split(",");
        return TableId.builder()
            .database(tableIdentifier[0])
            .table(tableIdentifier[1])
            .build();
    }

    public void close() {
        // Do nothing
    }
}
