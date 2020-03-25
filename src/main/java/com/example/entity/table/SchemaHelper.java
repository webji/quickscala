package com.example.entity.table;

import com.example.driver.FlinkDataTypes;
import com.example.util.Config;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaHelper {

    public static Schema fromList(List<Object> fields) {
        Schema ret = new Schema();

        for (Object fieldObject: fields) {
            Map<String, String> fieldMap = (Map<String, String>)fieldObject;
            ret.field(fieldMap.get("name"), fieldMap.get("type"));
        }
        return ret;
    }

    public static String[] fieldNames(List<Object> fields) {
        String[] ret = new String[fields.size()];
        List<String> nameList = new ArrayList<>();
        for (Object field: fields) {
            Map<String, String> fieldMap = (Map<String, String>)field;
            nameList.add(fieldMap.get("name"));
        }
        return nameList.toArray(ret);
    }

    public static DataType[] fieldTypes(List<Object> fields) {
        DataType[] ret = new DataType[fields.size()];
        List<DataType> typeList = new ArrayList<>();
        for (Object field: fields) {
            Map<String, String> fieldMap = (Map<String, String>)field;
            String type = fieldMap.get("type");
            DataType dataType = FlinkDataTypes.getDataTypeFromName(type);
            typeList.add(dataType);
        }
        return typeList.toArray(ret);
    }

//    public static TypeInformation[] typeInformations(List<Object> fields) {
//        List<TypeInformation> typeList = new ArrayList<>();
//        for (Object fieldObject: fields) {
//            Map<String, String> fieldMap = (Map<String, String>)fieldObject;
//            BasicTypeInfo.getInfoFor(String.class);
//        }
//    }
}
