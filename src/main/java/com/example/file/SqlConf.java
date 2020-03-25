package com.example.file;

import com.example.entity.source.Source;
import com.example.entity.source.SourceFactory;
import com.example.entity.table.Table;
import com.example.entity.table.TableFactory;
import com.example.entity.table.TableRole;
import com.example.util.Config;
import lombok.Data;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class SqlConf {
    String name;
    Config conf;
    List<Source> sources;
    List<Table> sourceTables;
    List<Table> dimTables;
    List<Table> sinkTables;


    public SqlConf(String name) {
        this.name = name;
        this.conf = load();
        this.sources = getDataSources();
        this.sourceTables = getDataTables("sourceTables");
        this.dimTables = getDataTables("dimTables");
        this.sinkTables = getDataTables("sinkTables");
    }

    public List<Table> getSourceTables() {
        return sourceTables;
    }

    public List<Table> getDimTables() {
        return dimTables;
    }

    public List<Table> getSinkTables() {
        return sinkTables;
    }

    private Config load() {
        Map<String, Object> conf = new HashMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            conf = mapper.readValue(new File(this.name), Map.class);
        } catch (IOException ioe) {
            System.out.println(ioe);
        }
        return new Config(conf);
    }

    private List<Source> getDataSources() {
        List<Source> sources = new ArrayList<>();
        List<Object> sourceMaps =this.conf.getList("dataSources");

        for (Object sourceMap : sourceMaps) {
            Source source = SourceFactory.fromMap((Map<String, Object>)sourceMap);
            sources.add(source);
        }
        return sources;
    }

    private List<Table> getDataTables(String tableType) {
        List<Table> tables = new ArrayList<>();
        List<Object> tableMaps = this.conf.getList(tableType);
        TableRole tableRole = TableRole.NA;
        if ("sourceTables".equalsIgnoreCase(tableType)) {
            tableRole = TableRole.SOURCE;
        } else if ("dimTables".equalsIgnoreCase(tableType)) {
            tableRole = TableRole.DIM;
        } else if ("sinkTables".equalsIgnoreCase(tableType)) {
            tableRole = TableRole.SINK;
        }

        for (Object tableMap : tableMaps) {
            Table table = TableFactory.fromMap((Map<String, Object>)tableMap, this.sources, tableRole);
            tables.add(table);
        }
        return tables;
    }
}
