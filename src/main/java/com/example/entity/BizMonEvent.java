package com.example.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class BizMonEvent extends MonitorEventBase {
    private String serviceName;
    private String itemName;
    private int value;

//    public static BizMonEvent fromString()

}
