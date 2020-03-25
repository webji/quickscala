package com.example.driver;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;

public enum SqlTypes {
    BIT("BIT", Types.BIT, boolean.class),
    TINYINT("TINYINT", Types.TINYINT, byte.class),
    SMALLINT("SMALLINT", Types.SMALLINT, short.class),
    INTEGER("INTEGER", Types.INTEGER, int.class),
    BIGINT("BIGINT", Types.BIGINT, long.class),
    FLOAT("FLOAT", Types.FLOAT, double.class),
    REAL("REAL", Types.REAL, float.class),
    DOUBLE("DOUBLE", Types.DOUBLE, double.class),
    NUMERIC("NUMERIC", Types.NUMERIC, BigDecimal.class),
    DECIMAL("DECIMAL", Types.DECIMAL, BigDecimal.class),
    CHAR("CHAR", Types.CHAR, String.class),
    VARCHAR("VARCHAR", Types.VARCHAR, String.class),
    LONGVARCHAR("LONGVARCHAR", Types.LONGVARCHAR, String.class),
    DATE("DATE", Types.DATE, Date.class),
    TIME("TIME", Types.TIME, Time.class),
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, Timestamp.class),
    BINARY("BINARY", Types.BINARY, byte[].class),
    VARBINARY("VARBINARY", Types.VARBINARY, byte[].class),
    LONGVARBINARY("LONGVARBINARY", Types.LONGVARBINARY, byte[].class),
//    NULL("NULL", Types.NULL, ),
//    OTHER("OTHER", Types.OTHER),
//    JAVA_OBJECT("JAVA_OBJECT", Types.JAVA_OBJECT),
//    DISTINCT("DISTINCT", Types.DISTINCT),
    STRUCT("STRUCT", Types.STRUCT, Struct.class),
    ARRAY("ARRAY", Types.ARRAY, Array.class),
    BLOB("BLOB", Types.BLOB, Blob.class),
    CLOB("CLOB", Types.CLOB, Clob.class),
    REF("REF", Types.REF, Ref.class),
    DATALINK("DATALINK", Types.DATALINK, URL.class),
    BOOLEAN("BOOLEAN", Types.BOOLEAN, boolean.class);
//    ROWID("ROWID", Types.ROWID, ),
//    NCHAR("NCHAR", Types.NCHAR),
//    NVARCHAR("NVARCHAR", Types.NVARCHAR),
//    LONGNVARCHAR("LONGNVARCHAR", Types.LONGNVARCHAR),
//    NCLOB("NCLOB", Types.NCLOB),
//    SQLXML("SQLXML", Types.SQLXML),
//    REF_CURSOR("REF_CURSOR", Types.REF_CURSOR),
//    TIME_WITH_TIMEZONE("TIME_WITH_TIMEZONE", Types.TIME_WITH_TIMEZONE),
//    TIMESTAMP_WITH_TIMEZONE("TIMESTAMP_WITH_TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE);

    private String name;
    private int value;
    private Class<?> clazz;

    private  SqlTypes(String name, int value, Class<?> clazz) {
        this.name = name;
        this.value = value;
        this.clazz = clazz;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getValue() {
        return this.value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public static String getNameFromValue(int value) {
        for (SqlTypes type : SqlTypes.values()) {
            if (type.getValue() == value) {
                return type.getName();
            }
        }
        return null;
    }

    public static int getValueFromName(String name) {
        for (SqlTypes type: SqlTypes.values()) {
            if (type.name.equals(name)) {
                return type.getValue();
            }
        }
        return Types.CHAR;
    }

}
