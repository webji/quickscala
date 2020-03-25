package com.example.entity.source;

import com.example.util.Config;
import lombok.Builder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Builder
public class MysqlSource extends JdbcSource implements DataSource {
    public static String driverClassName = "com.mysql.cj.jdbc.Driver";


    public static MysqlSource defaultSource() {
        MysqlSource mysqlSource = MysqlSource.builder().build();
        mysqlSource.type = SourceType.MYSQL;
        mysqlSource.name = "Mysql Table";
        mysqlSource.desc = "A Mysql Table for Demo";
        mysqlSource.jdbcUrl = "jdbc:mysql://localhost:33306/flink";
        mysqlSource.username = "root";
        mysqlSource.password = "abc123";
        return mysqlSource;
    }

    public static MysqlSource fromMap(Map<String, Object> confMap) {
        Config conf = new Config(confMap);
        MysqlSource source = MysqlSource.builder().build();
        source.id = conf.getInteger("id");
        source.name = conf.getString("name");
        source.desc = conf.getString("desc");
        source.jdbcUrl = conf.getString("jdbcUrl");
        source.username = conf.getString("username");
        source.password = conf.getString("password");
        source.type = SourceType.MYSQL;
        return source;
    }

    public Connection connect() throws Exception{
        Class.forName(driverClassName);
        return DriverManager.getConnection(jdbcUrl, username, password);
    }


}
