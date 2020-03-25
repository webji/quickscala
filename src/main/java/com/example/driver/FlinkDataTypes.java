package com.example.driver;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;

public enum FlinkDataTypes {
//    BIT("BIT", Types.BIT, DataTypes.BINARY(1)),
    TINYINT("TINYINT", Types.TINYINT, DataTypes.TINYINT()),
    SMALLINT("SMALLINT", Types.SMALLINT, DataTypes.SMALLINT()),
    INTEGER("INTEGER", Types.INTEGER, DataTypes.INT()),
    BIGINT("BIGINT", Types.BIGINT, DataTypes.BIGINT()),
    FLOAT("FLOAT", Types.FLOAT, DataTypes.FLOAT()),
    REAL("REAL", Types.REAL, DataTypes.DOUBLE()),
    DOUBLE("DOUBLE", Types.DOUBLE, DataTypes.DOUBLE()),
//    NUMERIC("NUMERIC", Types.NUMERIC, DataTypes.DECIMAL()),
//    DECIMAL("DECIMAL", Types.DECIMAL, DataTypes.DECIMAL()),
    CHAR("CHAR", Types.CHAR, DataTypes.STRING()),
    VARCHAR("VARCHAR", Types.VARCHAR, DataTypes.STRING()),
    LONGVARCHAR("LONGVARCHAR", Types.LONGVARCHAR, DataTypes.STRING()),
    DATE("DATE", Types.DATE, DataTypes.DATE()),
    TIME("TIME", Types.TIME, DataTypes.TIME()),
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, DataTypes.TIMESTAMP()),
    BINARY("BINARY", Types.BINARY, DataTypes.BYTES()),
    VARBINARY("VARBINARY", Types.VARBINARY, DataTypes.BYTES()),
    LONGVARBINARY("LONGVARBINARY", Types.LONGVARBINARY, DataTypes.BYTES()),
    NULL("NULL", Types.NULL, DataTypes.NULL()),
//    OTHER("OTHER", Types.OTHER, DataType),
//    JAVA_OBJECT("JAVA_OBJECT", Types.JAVA_OBJECT),
//    DISTINCT("DISTINCT", Types.DISTINCT),
//    STRUCT("STRUCT", Types.STRUCT, DataTypes.ST),
//    ARRAY("ARRAY", Types.ARRAY, Array.class),
//    BLOB("BLOB", Types.BLOB, DataTypes.),
//    CLOB("CLOB", Types.CLOB, Clob.class),
//    REF("REF", Types.REF, Ref.class),
//    DATALINK("DATALINK", Types.DATALINK, URL.class),
    BOOLEAN("BOOLEAN", Types.BOOLEAN, DataTypes.BOOLEAN()),
//    ROWID("ROWID", Types.ROWID, ),
//    NCHAR("NCHAR", Types.NCHAR),
//    NVARCHAR("NVARCHAR", Types.NVARCHAR),
//    LONGNVARCHAR("LONGNVARCHAR", Types.LONGNVARCHAR),
//    NCLOB("NCLOB", Types.NCLOB),
//    SQLXML("SQLXML", Types.SQLXML),
//    REF_CURSOR("REF_CURSOR", Types.REF_CURSOR),
//    TIME_WITH_TIMEZONE("TIME_WITH_TIMEZONE", Types.TIME_WITH_TIMEZONE, DataTypes.TIME),
    TIMESTAMP_WITH_TIMEZONE("TIMESTAMP_WITH_TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE, DataTypes.TIMESTAMP_WITH_TIME_ZONE());

    private String name;
    private int value;
    private DataType type;

    private FlinkDataTypes(String name, int value, DataType type) {
        this.name = name;
        this.value = value;
        this.type = type;
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

    public DataType getType() {
        return this.type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public static String getNameFromValue(int value) {
        for (FlinkDataTypes type : FlinkDataTypes.values()) {
            if (type.getValue() == value) {
                return type.getName();
            }
        }
        return null;
    }

    public static int getValueFromName(String name) {
        for (FlinkDataTypes type: FlinkDataTypes.values()) {
            if (type.name.equals(name)) {
                return type.getValue();
            }
        }
        return Types.CHAR;
    }

    public static DataType getDataTypeFromName(String name) {
        for (FlinkDataTypes type: FlinkDataTypes.values()) {
            if (type.name.equals(name)) {
                return type.getType();
            }
        }
        return DataTypes.NULL();
    }

}
