package com.example.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.joda.time.DateTime;

import java.io.Serializable;

@Data
@ToString
@AllArgsConstructor
public abstract class MonitorEventBase implements Serializable {
    public DateTime timestamp;
    public String ip;

    public MonitorEventBase() {
        this.timestamp = new DateTime();
        this.ip = "0.0.0.0";
    }

}
