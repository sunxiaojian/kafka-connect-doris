/**
 *
 */
package org.apache.doris.kafka.connect.sink;

import org.apache.doris.kafka.connect.common.VersionUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

/**
 * kafka connect doris sink task
 */
public class DorisSinkTask extends SinkTask {

    public void start(Map<String, String> map) {

    }

    public void put(Collection<SinkRecord> collection) {

    }

    public void stop() {

    }

    public String version() {
        return VersionUtil.getVersion();
    }
}
