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

package org.apache.doris.kafka.connect.sink.converter;

import com.google.common.collect.Maps;
import org.apache.doris.kafka.connect.common.JsonUtils;
import org.apache.doris.kafka.connect.utils.DateTimeUtils;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

public class JsonRowConverter implements RowConverter<String> {

    private final TimeZone timeZone;

    public JsonRowConverter(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public String convert(SinkRecord record) {
        Map<String, Object> data = Maps.newConcurrentMap();
        List<Field> fields = record.valueSchema().fields();
        Struct value = (Struct) record.value();
        fields.forEach(field -> {
            Optional<Object> columnValue = maybeBindLogical(field.schema(), value.get(field));
            if (columnValue.isPresent()) {
                data.put(field.name(), columnValue.get());
                return;
            }
            columnValue = maybeDebeziumLogical(field.schema(), value.get(field));
            if (columnValue.isPresent()) {
                data.put(field.name(), columnValue.get());
                return;
            }
            if (columnValue.isPresent()) {
                data.put(field.name(), columnValue.get());
                return;
            }
            data.put(field.name(), convert(field, value.get(field)));
        });
        return JsonUtils.toJsonString(data);
    }

    private Optional<Object> maybeDebeziumLogical(Schema schema, Object o) {
        return Optional.empty();
    }

    private Object convert(Field field, Object value) {
        if (value == null) {
            return null;
        }
        switch (field.schema().type()) {
            case INT8:
            case BOOLEAN:
            case FLOAT64:
            case INT32:
            case INT64:
            case FLOAT32:
            case STRING:
                return value;
            case BYTES:
                if (value instanceof BigDecimal) {
                    return value;
                }
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                return new String(bytes);
            case MAP:
            case ARRAY:
                return JsonUtils.toJsonString(value);
            default:
                throw new UnsupportedOperationException("Unsupported type " + field.schema().type());
        }
    }


    protected Optional<Object> maybeBindLogical(Schema schema, Object value) {
        if (schema.name() != null) {
            switch (schema.name()) {
                case Decimal.LOGICAL_NAME:
                    return Optional.of(value);
                case Date.LOGICAL_NAME:
                    java.util.Date date;
                    if (value instanceof java.util.Date) {
                        date = new java.util.Date(((java.util.Date) value).getTime());
                    } else {
                        date = Date.toLogical(schema, (Integer) value);
                    }
                    return Optional.of(DateTimeUtils.formatTime(date, this.timeZone));
                case Time.LOGICAL_NAME:
                    java.util.Date time;
                    if (value instanceof java.util.Date) {
                        time = (java.util.Date) value;
                    } else {
                        time = Time.toLogical(schema, (int) value);
                    }
                    return Optional.of(DateTimeUtils.formatTime(time, this.timeZone));
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    Timestamp timestamp;
                    if (value instanceof java.util.Date) {
                        timestamp = new Timestamp(((java.util.Date) value).getTime());
                    } else {
                        timestamp = new Timestamp((long) value);
                    }
                    return Optional.of(DateTimeUtils.formatTimestamp(timestamp, this.timeZone));
            }
        }
        return Optional.empty();
    }

}
