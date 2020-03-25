package com.example.driver;

import com.example.entity.source.MysqlSource;
import com.example.entity.table.MysqlDimTable;
import com.example.entity.table.MysqlSinkTable;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

public class MysqlDimSource extends RichSourceFunction<Map<String, Object>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    MysqlDimTable dimTable;
    MysqlSource source;
//    JDBC

//    JDBCTableSource jdbcTableSource = JDBCTableSource.builder().build();

//    JDBCInputFormat jdbcInputFormat =

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = source.connect();
        String sql = dimTable.dimSql();
        System.out.println(sql);
        preparedStatement = connection.prepareStatement(sql);
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<Map<String, Object>> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
