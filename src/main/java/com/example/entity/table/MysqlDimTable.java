package com.example.entity.table;

import com.example.entity.source.MysqlSource;
import com.example.entity.source.Source;
import com.example.entity.source.SourceBase;
import com.example.util.Config;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCLookupOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class MysqlDimTable extends SourceTableBase implements Table{
    String table;
    String mapper;
    List<Object> schema;
    String pk;
    int parallel;
    CacheStrategy cacheStrategy;
    int cacheLines;
    int cacheTime;
    boolean partitionEnabled;
    int errorTolerance;

    public String getMapper() {
        return mapper;
    }

    public String getPk() {
        return pk;
    }


    public static MysqlDimTable defaultTable() {
        MysqlDimTable mysqlDimTable = MysqlDimTable.builder()
                .table("flink_demo_dim")
                .mapper("flink_demo_dim")
                .pk("id")
                .parallel(1)
                .cacheStrategy(CacheStrategy.NONE)
                .cacheLines(0)
                .cacheTime(0)
                .partitionEnabled(false)
                .errorTolerance(0)
                .build();
        mysqlDimTable.source = MysqlSource.defaultSource();
        return mysqlDimTable;
    }

    public static MysqlDimTable fromMap(Map<String, Object> confMap, Source source) {
        Config conf = new Config(confMap);

        MysqlDimTable table = MysqlDimTable.builder()
                .table(conf.getString("table"))
                .mapper(conf.getString("mapper"))
                .schema(conf.getList("schema"))
                .pk(conf.getString("pk"))
                .parallel(conf.getInteger("parallel"))
                .cacheStrategy(CacheStrategy.NONE)
                .cacheLines(conf.getInteger("cacheLines"))
                .cacheTime(conf.getInteger("cacheTime"))
                .partitionEnabled(conf.getBoolean("partitionEnabled"))
                .errorTolerance(conf.getInteger("errorTolerance"))
                .build();

        table.id = conf.getInteger("id");
        table.source = source;
        table.tableRole = TableRole.DIM;

        return table;
    }

    public String dimSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        int i = 0;
        for (Object field: schema) {
            Map<String, String> fieldMap = (Map<String, String>)field;
            String name = fieldMap.get("name");
            if (i < schema.size() - 1) {
                sb.append(name + ", ");
                i++;
            } else {
                sb.append(name + " ");
            }
        }
        sb.append(table);
        return sb.toString();
    }

    public JDBCTableSource jdbcTableSource() {
        MysqlSource mysqlSource = (MysqlSource)source;
        String[] fieldNames = SchemaHelper.fieldNames(schema);
        DataType[] fieldTypes = SchemaHelper.fieldTypes(schema);
        TableSchema tableSchema = TableSchema.builder().fields(fieldNames, fieldTypes).build();
        JDBCOptions options = JDBCOptions.builder()
                .setDriverName(MysqlSource.driverClassName)
                .setDBUrl(mysqlSource.getJdbcUrl())
                .setUsername(mysqlSource.getUsername())
                .setPassword(mysqlSource.getPassword())
                .setTableName(table)
                .build();
        JDBCLookupOptions lookupOptions = JDBCLookupOptions.builder()
                .setCacheExpireMs(cacheTime)
                .setCacheMaxSize(cacheLines)
                .setMaxRetryTimes(errorTolerance)
                .build();

        JDBCTableSource tableSource = JDBCTableSource.builder()
                .setOptions(options)
                .setLookupOptions(lookupOptions)
                .setSchema(tableSchema)
                .build();
        return tableSource;
    }

}
