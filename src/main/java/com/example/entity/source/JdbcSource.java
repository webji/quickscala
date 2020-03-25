package com.example.entity.source;

import lombok.Data;

@Data
public class JdbcSource extends SourceBase {
    String jdbcUrl;
    String username;
    String password;
}
