package com.example.entity.source;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

@Data
public class SourceBase implements Serializable {
    Integer id;
    SourceType type;
    String name;
    String desc;

}
