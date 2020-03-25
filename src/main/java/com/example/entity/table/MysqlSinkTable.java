package com.example.entity.table;

import com.example.entity.source.MysqlSource;
import com.example.entity.source.Source;
import com.example.util.Config;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.table.descriptors.Schema;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class MysqlSinkTable extends SourceTableBase implements Table {
    String table;
    String mapper;
    List<Object> schema;
    UpdateMode mode;
    int parallel;
    int outInterval;
    int outCount;

    public static MysqlSinkTable defaultTable() {
        MysqlSinkTable mysqlSinkTable = MysqlSinkTable.builder()
                .table("flink_sql_demo")
                .mapper("flink_sql_demo")
                .mode(UpdateMode.UPDATE)
                .parallel(1)
                .outInterval(10)
                .outCount(5)
                .build();
        mysqlSinkTable.source = MysqlSource.defaultSource();
        return mysqlSinkTable;
    }

    public static MysqlSinkTable fromMap(Map<String, Object> confMap, Source source) {
        Config conf = new Config(confMap);
        MysqlSinkTable table = MysqlSinkTable.builder()
                .table(conf.getString("table"))
                .mapper(conf.getString("mapper"))
                .schema(conf.getList("schema"))
                .parallel(conf.getInteger("parallel"))
                .outInterval(conf.getInteger("outInterval"))
                .outCount(conf.getInteger("outCount"))
                .build();

        table.id = conf.getInteger("id");
        table.source = source;
        table.tableRole = TableRole.SINK;
        return table;
    }

    public String sinkSql() {
        StringBuilder sb = new StringBuilder();
        if (mode == UpdateMode.APPEND) {
            sb.append("INSERT INTO ");
        } else {
            sb.append("REPLACE INTO ");
        }
        sb.append(table + "(");
        int i = 0;
        for (Object field: schema) {
            Map<String, String> fieldMap = (Map<String, String>)field;
            String name = fieldMap.get("name");
            if (i < schema.size() - 1) {
                sb.append(name + ", ");
                i++;
            } else {
                sb.append(name + ") VALUES(");
                i++;
            }
        }
        for (; i>0; i--) {
            if (i > 1) {
                sb.append("?, ");
            } else {
                sb.append("?)");
            }
        }
        return sb.toString();
    }
}
