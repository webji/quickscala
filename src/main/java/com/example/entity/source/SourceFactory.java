package com.example.entity.source;

import com.example.util.Config;
import lombok.AllArgsConstructor;
import org.apache.flink.table.descriptors.Kafka;

import java.util.Map;

@AllArgsConstructor
public class SourceFactory {
    Config conf;

    public static Source fromMap(Map<String, Object> confMap) {
        Config conf = new Config(confMap);
        String type = conf.getString("type");
        if ("KAFKA_UNIVERSAL".equalsIgnoreCase(type)) {
            return KafkaSource.fromMap(confMap);
        } else if ("MYSQL".equalsIgnoreCase(type)) {
            return MysqlSource.fromMap(confMap);
        }
        return new DummySource();
    }
}
