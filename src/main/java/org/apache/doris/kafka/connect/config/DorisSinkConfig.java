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
package org.apache.doris.kafka.connect.config;

import org.apache.doris.kafka.connect.common.DataFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.TimeZone;

/**
 * Doris sink config
 */
public class DorisSinkConfig extends AbstractConfig {

    private final static String FE_HOST = "host";
    private final static String FE_HOST_DESCRIPTION = "doris fe http address";

    private final static String USERNAME = "username";
    private final static String PASSWORD = "password";

    private final static String TABLE_IDENTIFIER = "table.identifier";
    private final static String TABLE_IDENTIFIER_DESCRIPTION = "The doris table name.";

    private final static String ENABLED_2PC = "enable.2pc";
    private final static Boolean ENABLED_2PC_DEFAULT = false;
    private final static String ENABLED_2PC_DESCRIPTION = "enable 2PC while loading";

    private final static String ENABLED_DELETED = "enable.delete";
    private final static Boolean ENABLED_DELETED_DEFAULT = false;
    private final static String ENABLED_DELETED_DESCRIPTION = "enable 2PC while loading";

    // Retry config
    private final static String MAX_RETRIES = "max.retries";
    private final static Integer MAX_RETRIES_DEFAULT = 3;

    private final static String CHECK_INTERVAL = "check.interval";
    private final static Integer CHECK_INTERVAL_DEFAULT = 3000;

    private final static String BUFFER_SIZE = "buffer.size";
    private final static Integer BUFFER_SIZE_DEFAULT = 10;

    private final static String LABEL_PREFIX = "label.prefix";

    // format type
    private final static String FORMAT = "format";
    private final static String FORMAT_DEFAULT = DataFormat.JSON.name();

    // column separator
    private static final String FIELD_DELIMITER_KEY = "column.separator";
    private static final String FIELD_DELIMITER_DEFAULT = "\t";

    // line delimiter
    private static final String LINE_DELIMITER_KEY = "line.delimiter";
    private static final String LINE_DELIMITER_DEFAULT = "\n";


    private static final String TIMEZONE = "timezone";

    // where
    private static final String WHERE = "where";
    public static final ConfigDef conf = new ConfigDef()
        .define(FE_HOST, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FE_HOST_DESCRIPTION)
        .define(USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "")
        .define(PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "")
        .define(TABLE_IDENTIFIER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TABLE_IDENTIFIER_DESCRIPTION)
        .define(ENABLED_2PC, ConfigDef.Type.BOOLEAN, ENABLED_2PC_DEFAULT, ConfigDef.Importance.HIGH, ENABLED_2PC_DESCRIPTION)
        .define(ENABLED_DELETED, ConfigDef.Type.BOOLEAN, ENABLED_DELETED_DEFAULT, ConfigDef.Importance.HIGH, ENABLED_DELETED_DESCRIPTION)
        .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, ConfigDef.Importance.HIGH, "")
        .define(CHECK_INTERVAL, ConfigDef.Type.INT, CHECK_INTERVAL_DEFAULT, ConfigDef.Importance.HIGH, "")
        .define(BUFFER_SIZE, ConfigDef.Type.INT, BUFFER_SIZE_DEFAULT, ConfigDef.Importance.HIGH, "")
        .define(LABEL_PREFIX, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "")
        .define(FORMAT, ConfigDef.Type.STRING, FORMAT_DEFAULT, ConfigDef.Importance.HIGH, "")
        .define(FIELD_DELIMITER_KEY, ConfigDef.Type.STRING, FIELD_DELIMITER_DEFAULT, ConfigDef.Importance.HIGH, "")
        .define(LINE_DELIMITER_KEY, ConfigDef.Type.STRING, LINE_DELIMITER_DEFAULT, ConfigDef.Importance.HIGH, "")
        .define(WHERE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "")
        .define(TIMEZONE, ConfigDef.Type.STRING, "UTC", ConfigDef.Importance.HIGH, "");
    // common option
    private final String host;
    private final String username;
    private final String password;
    private final String tableIdentifier;
    // sink option
    private final Boolean enable2PC;
    private final Boolean enableDelete;
    private final String labelPrefix;
    private final Integer checkInterval;
    private final Integer maxRetries;
    private final Integer bufferSize;
    private final DataFormat format;
    private final String columnSeparator;
    private final String lineDelimiter;
    private final String where;
    private final TimeZone timeZone;

    public DorisSinkConfig(Map<String, String> config) {
        super(conf, config);
        // Not empty config
        this.host = getString(FE_HOST);
        this.username = getString(USERNAME);
        this.password = getPassword(PASSWORD).value();
        this.tableIdentifier = getString(TABLE_IDENTIFIER);

        // option config
        this.enable2PC = getBoolean(ENABLED_2PC);
        this.enableDelete = getBoolean(ENABLED_DELETED);
        this.maxRetries = getInt(MAX_RETRIES);
        this.checkInterval = getInt(CHECK_INTERVAL);
        this.labelPrefix = getString(LABEL_PREFIX);

        this.bufferSize = getInt(BUFFER_SIZE);
        this.format = DataFormat.valueOf(getString(FORMAT).toUpperCase());
        this.columnSeparator = getString(FIELD_DELIMITER_KEY);
        this.lineDelimiter = getString(LINE_DELIMITER_KEY);
        this.where = getString(WHERE);
        this.timeZone = getString(TIMEZONE) == null ? TimeZone.getDefault() : TimeZone.getTimeZone(getString(TIMEZONE));
    }

    public String getHost() {
        return host;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public Boolean getEnable2PC() {
        return enable2PC;
    }

    public Boolean getEnableDelete() {
        return enableDelete;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    public Integer getCheckInterval() {
        return checkInterval;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public Integer getBufferSize() {
        return bufferSize;
    }

    public DataFormat getFormat() {
        return format;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public String getWhere() {
        return where;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }
}
