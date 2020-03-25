package com.example.driver;

import com.example.entity.source.MysqlSource;
import com.example.entity.table.MysqlSinkTable;
import com.example.entity.table.UpdateMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

public class MysqlStreamSink extends RichSinkFunction<Row> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    MysqlSinkTable sinkTable;
    MysqlSource source;

    public MysqlStreamSink(MysqlSinkTable sinkTable) {
        this.sinkTable = sinkTable;
        this.source = (MysqlSource)sinkTable.getSource();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = source.connect();
        String sql = sinkTable.sinkSql();
        System.out.println(sql);
        preparedStatement = connection.prepareStatement(sql);
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        List<Object> schema = sinkTable.getSchema();
        for (int i=0; i<value.getArity(); i++) {
            Object field = value.getField(i);
            System.out.println(i + " " + field);
            Map<String, String> fieldMap = (Map<String, String>)schema.get(i);
            int sqlType = SqlTypes.getValueFromName(fieldMap.get("type"));
            System.out.println(i + " " + field + " " + sqlType);
            preparedStatement.setObject(i+1, field, sqlType);
        }
        int i = preparedStatement.executeUpdate();
        if (i > 0) {
            System.out.println("value=" + value);
        } else {
            System.out.println("error");
        }
    }
}
