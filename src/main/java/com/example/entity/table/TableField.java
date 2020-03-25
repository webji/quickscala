package com.example.entity.table;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TableField {
    String name;
    String type;
}
