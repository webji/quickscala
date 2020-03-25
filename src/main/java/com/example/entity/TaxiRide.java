package com.example.entity;

import com.example.util.DateUtil;
import com.example.util.GeoUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.joda.time.DateTime;

import java.io.Serializable;


@Data
@AllArgsConstructor
@ToString
public class TaxiRide implements Comparable<TaxiRide>, Serializable {

    public long rideId;
    public boolean isStart;
    public DateTime startTime;
    public DateTime endTime;
    public float startLon;
    public float startLat;
    public float endLon;
    public float endLat;
    public short passengerCnt;
    public long taxiId;
    public long driverId;

    public TaxiRide() {
        this.startTime = new DateTime();
        this.endTime = new DateTime();
    }

    public static TaxiRide fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 11) {
            throw new RuntimeException("Invalid Record: " + line);
        }

        TaxiRide ride = new TaxiRide();

        try {
            ride.rideId = Long.parseLong(tokens[0]);

            switch (tokens[1]) {
                case "START":
                    ride.isStart = true;
                    ride.startTime = DateTime.parse(tokens[2], DateUtil.timeFormatter);
                    ride.endTime = DateTime.parse(tokens[3], DateUtil.timeFormatter);
                    break;
                case "END":
                    ride.isStart = false;
                    ride.endTime = DateTime.parse(tokens[2], DateUtil.timeFormatter);
                    ride.startTime = DateTime.parse(tokens[3], DateUtil.timeFormatter);
                    break;
                default:
                    throw new RuntimeException("Invalid Record: " + line);
            }

            ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0F;
            ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0F;
            ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0F;
            ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0F;
            ride.passengerCnt = Short.parseShort(tokens[8]);
            ride.taxiId = Long.parseLong(tokens[9]);
            ride.driverId = Long.parseLong(tokens[10]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid Record: " + line, nfe);
        }

        return ride;
    }


    @Override
    public int compareTo(TaxiRide o) {
        if (o == null) {
            return 1;
        }
        int compareTimes = Long.compare(this.getEventTime(), o.getEventTime());
        if (compareTimes == 0) {
            if (this.isStart == o.isStart) {
                return 0;
            } else {
                return isStart ? -1 : 1;
            }
        } else {
            return compareTimes;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaxiRide taxiRide = (TaxiRide) o;

        return rideId == taxiRide.rideId;
    }

    @Override
    public int hashCode() {
        return (int) (rideId ^ (rideId >>> 32));
    }

    public long getEventTime() {
        return isStart ? startTime.getMillis() : endTime.getMillis();
    }

    public double getEuclideanDistance(double longitude, double latitude) {
        return isStart ? GeoUtil.getEuclideanDistance((float) longitude, (float) latitude, this.startLon, this.startLat)
                : GeoUtil.getEuclideanDistance((float) longitude, (float) latitude, this.endLon, this.endLat);
    }
}
