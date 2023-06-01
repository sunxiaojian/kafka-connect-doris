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
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.kafka.connect.config.DorisSinkConfig;
import org.apache.doris.kafka.connect.sink.converter.ConverterFactory;
import org.apache.doris.kafka.connect.sink.converter.RowConverter;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.isNull;

@Slf4j
public class BufferRecords implements Closeable {
    private Schema keySchema;
    private Schema valueSchema;
    private List<SinkRecord> bufferRecords;
    private final DorisSinkConfig config;
    private boolean deleteEnabled;
    private final DorisStreamLoad streamLoad;

    public BufferRecords(DorisSinkConfig config) {
        this.config = config;
        this.bufferRecords = new ArrayList<>();
        this.streamLoad = new DorisStreamLoad(config);
    }

    public List<SinkRecord> add(SinkRecord record) throws IOException {
        final List<SinkRecord> flushed = new ArrayList<>();
        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.keySchema())) {
            keySchema = record.keySchema();
            schemaChanged = true;
        }
        if (isNull(record.valueSchema())) {
            if (config.getEnableDelete()) {
                deleteEnabled = true;
            }
        } else if (Objects.equals(valueSchema, record.valueSchema())) {
            if (config.getEnableDelete() && deleteEnabled) {
                flushed.addAll(flush());
            }
        } else {
            valueSchema = record.valueSchema();
            schemaChanged = true;
        }
        if (schemaChanged) {
            // Each batch needs to have the same schemas, so get the buffered records out
            flushed.addAll(flush());
        }

        // set deletesInBatch if schema value is not null
        if (isNull(record.value()) && config.getEnableDelete()) {
            deleteEnabled = true;
        }

        bufferRecords.add(record);
        if (bufferRecords.size() >= config.getBufferSize()) {
            flushed.addAll(flush());
        }
        return flushed;
    }

    public List<SinkRecord> flush() throws IOException {
        if (bufferRecords.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", bufferRecords.size());
        List<SinkRecord> deleteRecords = Lists.newArrayList();
        List<SinkRecord> updateRecords = Lists.newArrayList();
        for (SinkRecord record : bufferRecords) {
            if (isNull(record.value())) {
                deleteRecords.add(record);
            } else {
                updateRecords.add(record);
            }
        }
        executeUpdates(updateRecords);
        executeDeletes(deleteRecords);
        final List<SinkRecord> flushedRecords = bufferRecords;
        bufferRecords = new ArrayList<>();
        return flushedRecords;
    }

    private void executeDeletes(List<SinkRecord> records) {
        // TODO: unsupported
    }

    private void executeUpdates(List<SinkRecord> records) throws IOException {
        StringBuilder builder = new StringBuilder();
        RowConverter converter = ConverterFactory.getConverter(config.getFormat(), config.getTimeZone());
        boolean isFirstRecord = true;
        for (SinkRecord record : records) {
            if (isFirstRecord) {
                isFirstRecord = false;
            } else {
                builder.append(config.getLineDelimiter());
            }
            builder.append(converter.convert(record));
        }
        streamLoad.writeRecord(builder.toString(), config.getLabelPrefix());
    }

    private String generateLabel(){
        return  config.getLabelPrefix()+"_"+System.currentTimeMillis();
    }

    @Override
    public void close() throws IOException {
        if (streamLoad != null) {
            streamLoad.close();
        }
    }
}
