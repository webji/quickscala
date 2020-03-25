package com.example.entity.table;

import com.example.entity.source.Source;
import com.example.entity.source.SourceBase;
import com.example.entity.source.SourceType;
import lombok.Data;

import java.io.Serializable;

@Data
public class SourceTableBase implements Serializable {
    Integer id;
    Source source;
    TableRole tableRole;

    public Source getSource() {
        return this.source;
    }
}
