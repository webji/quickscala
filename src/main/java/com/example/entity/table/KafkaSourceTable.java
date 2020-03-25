package com.example.entity.table;

import com.example.entity.source.KafkaSource;
import com.example.entity.source.Source;
import com.example.util.Config;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.table.descriptors.Schema;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class KafkaSourceTable extends SourceTableBase implements Table {
    String topic;
    String tableName;
    List<Object> schema;
    OffsetType offset;
    TimeCharacteristic timeType;
    int parallel;
    String timezone;
    List<KVParam> customizedParams;

    public String getTopic() {
        return this.topic;
    }

    public String getTableName() {
        return this.tableName;
    }

    public TimeCharacteristic getTimeType() {
        return this.timeType;
    }

    public List<Object> getSchema() {
        return this.schema;
    }

    public OffsetType getOffset() {
        return this.offset;
    }
    public int getParallel() {
        return  this.parallel;
    }


    public static KafkaSourceTable defaultTable() {
        KafkaSourceTable sourceTable = KafkaSourceTable.builder()
                .topic("console")
                .tableName("flink_kafka_table")
                .offset(OffsetType.LATEST)
                .timeType(TimeCharacteristic.ProcessingTime)
                .parallel(1)
                .timezone("Beijing")
                .build();

        sourceTable.source = KafkaSource.defaultSource();

        return  sourceTable;
    }

    public static KafkaSourceTable fromMap(Map<String, Object> confMap, Source source) {
        Config conf = new Config(confMap);
        KafkaSourceTable table = KafkaSourceTable.builder()
                .topic(conf.getString("topic"))
                .tableName(conf.getString("tableName"))
                .schema(conf.getList("schema"))
                .parallel(conf.getInteger("parallel"))
                .build();
        String offset = conf.getString("offset");
        if ("latest".equalsIgnoreCase(offset)) {
            table.offset = OffsetType.LATEST;
        } else {
            table.offset = OffsetType.EARLIEST;
        }

        String timeType = conf.getString("timeType");
        if ("ProcTime".equalsIgnoreCase(timeType)) {
            table.timeType = TimeCharacteristic.ProcessingTime;
        } else {
            table.timeType = TimeCharacteristic.EventTime;
        }

//        table.schema = SchemaHelper.fromList(conf.getList("schema"));
        table.id = conf.getInteger("id");

        table.source = source;
        table.tableRole = TableRole.SOURCE;

        return table;
    }
}
