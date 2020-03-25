package com.example.util;

import com.example.entity.TaxiFare;
import com.example.entity.TaxiRide;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class DataUtil {
    public static SourceFunction<TaxiRide> rides = null;
    public static SourceFunction<TaxiFare> fares = null;
    public static SourceFunction<String> strings = null;
    public static SinkFunction out = null;
    public static int parallelism = 2;

    public final static String pathToRideData = "D:\\Development\\Study\\Flink\\Code\\quickscala\\data\\nycTaxiRides.gz";
    public final static String pathToFareData = "D:\\Development\\Study\\Flink\\Code\\quickscala\\data\\nycTaxiFares.gz";

    public static SourceFunction<TaxiRide> rideSourceOrTest(SourceFunction<TaxiRide> source) {
        return rides == null ? source : rides;
    }

    public static SourceFunction<TaxiFare> fareSourceOrTest(SourceFunction<TaxiFare> source) {
        return fares == null ? source : fares;
    }

    public static SourceFunction<String> stringSourceOrTest(SourceFunction<String> source) {
        return strings == null ? source : strings;
    }

    public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }
    public static void printOrTest(org.apache.flink.streaming.api.scala.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }



}
