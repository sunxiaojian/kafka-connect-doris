package org.apache.doris.kafka.connect.sink;

import org.apache.doris.kafka.connect.common.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

/**
 * kafka connect doris sink connector
 */
public class DorisSinkConnector extends SinkConnector {

    public void start(Map<String, String> map) {

    }

    public Class<? extends Task> taskClass() {
        return null;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        return null;
    }

    public void stop() {

    }

    public ConfigDef config() {
        return null;
    }

    public String version() {
        return VersionUtil.getVersion();
    }
}
