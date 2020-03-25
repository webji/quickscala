package com.example.entity.table;

import com.example.entity.source.DummySource;
import com.example.entity.source.Source;
import com.example.entity.source.SourceType;
import com.example.util.Config;

import java.util.List;
import java.util.Map;

public class TableFactory {

    public static Table fromMap(Map<String, Object> confMap, List<Source> sources, TableRole tableRole) {
        Table table = null;

        Config conf = new Config(confMap);
        Integer sourceId = conf.getInteger("sourceId");

        Source sourceRef = new DummySource();
        for (Source source : sources) {
            if (sourceId == source.getId()) {
                sourceRef = source;
            }
        }

        if (sourceRef.getType() == SourceType.KAFKA_UNIVERSAL) {
            if (tableRole == TableRole.SOURCE) {
                table = KafkaSourceTable.fromMap(confMap, sourceRef);
            }
        } else if (sourceRef.getType() == SourceType.MYSQL) {
            if (tableRole == TableRole.DIM) {
                table = MysqlDimTable.fromMap(confMap, sourceRef);
            } else if (tableRole == TableRole.SINK) {
                table = MysqlSinkTable.fromMap(confMap, sourceRef);
            }
        }

        return table;
    }

}
