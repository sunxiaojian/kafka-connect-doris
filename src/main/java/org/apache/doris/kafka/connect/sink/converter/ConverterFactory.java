package org.apache.doris.kafka.connect.sink.converter;

import org.apache.doris.kafka.connect.common.DataFormat;

import java.util.TimeZone;

public class ConverterFactory {
    public static RowConverter getConverter(DataFormat format, TimeZone timeZone){
        switch (format) {
            case JSON:
                return new JsonRowConverter(timeZone);
            default:
                return new CsvRowConverter();
        }
    }
}
