package com.example.entity;

import com.example.util.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.joda.time.DateTime;

import java.io.Serializable;

@Data
@AllArgsConstructor
@ToString
public class TaxiFare implements Serializable {

    public long rideId;
    public long taxiId;
    public long driverId;
    public DateTime startTime;
    public String paymentType;
    public float tip;
    public float tolls;
    public float totalFare;

    public TaxiFare() { this.startTime = new DateTime(); }

    public static TaxiFare fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 8) {
            throw new RuntimeException("Invalid Record: " + line);
        }

        TaxiFare ride = new TaxiFare();

        try {
            ride.rideId = Long.parseLong(tokens[0]);
            ride.taxiId = Long.parseLong(tokens[1]);
            ride.driverId = Long.parseLong(tokens[2]);
            ride.startTime = DateTime.parse(tokens[3], DateUtil.timeFormatter);
            ride.paymentType = tokens[4];
            ride.tip = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0F;
            ride.tolls = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0F;
            ride.totalFare = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0F;

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid Record: " + line, nfe);
        }
        return ride;
    }

    public long getEventTime() {
        return startTime.getMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaxiFare taxiFare = (TaxiFare) o;

        return rideId == taxiFare.rideId;
    }

    @Override
    public int hashCode() {
        return (int) (rideId ^ (rideId >>> 32));
    }
}
