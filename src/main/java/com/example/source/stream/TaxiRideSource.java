package com.example.source.stream;

import com.example.entity.TaxiRide;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;

public class TaxiRideSource implements SourceFunction<TaxiRide> {

    private final int maxDelayMSecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    public TaxiRideSource(String dataFilePath) { this(dataFilePath, 0, 1); }
    public TaxiRideSource(String dataFilePath, int servingSpeedFactor) { this(dataFilePath, 0, servingSpeedFactor); }

    public TaxiRideSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if (maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }

        this.dataFilePath = dataFilePath;
        this.maxDelayMSecs = maxEventDelaySecs;
        this.watermarkDelayMSecs = maxDelayMSecs < 10000 ? 10000 : maxDelayMSecs;
        this.servingSpeed = servingSpeedFactor;
    }


    @Override
    public void run(SourceContext<TaxiRide> ctx) throws Exception {
        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

        generateUnorderedStream(ctx);

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;
    }

    private void generateUnorderedStream(SourceContext<TaxiRide> context) throws Exception {
        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        Random rand = new Random(7452);
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(32,
                new Comparator<Tuple2<Long, Object>>() {
                    @Override
                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                });

        String line;
        TaxiRide ride;
        if (reader.ready() && (line = reader.readLine()) != null) {
            ride = TaxiRide.fromString(line);
            dataStartTime = ride.getEventTime();
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMSecs -1);
            emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
        } else {
            return;
        }

        if (reader.ready() && (line = reader.readLine()) != null) {
            ride = TaxiRide.fromString(line);
        }

        while (emitSchedule.size() > 0 || reader.ready()) {
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long rideEventTime = ride != null ? ride.getEventTime() : -1;
            while (
                    ride != null && ( emitSchedule.isEmpty() || rideEventTime < curNextDelayedEventTime + maxDelayMSecs )
            ) {
                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

                if (reader.ready() && (line = reader.readLine()) != null) {
                    ride = TaxiRide.fromString(line);
                    rideEventTime = ride.getEventTime();
                } else {
                    ride = null;
                    rideEventTime = -1;
                }
            }

            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;

            long now = Calendar.getInstance().getTimeInMillis();
            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            long waitTime = servingTime - now;

            Thread.sleep((waitTime > 0) ? waitTime : 0);

            if (head.f1 instanceof TaxiRide) {
                TaxiRide emitRide = (TaxiRide)head.f1;
                context.collectWithTimestamp(emitRide, emitRide.getEventTime());
            } else if (head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark)head.f1;

                context.emitWatermark(emitWatermark);
                long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                Watermark nextWaterMark = new Watermark(watermarkTime - maxDelayMSecs - 1);
                emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWaterMark));
            }
        }
    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTIme) {
        long dataDiff = eventTIme - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMSecs / 2;
        while (delay < 0 || delay > maxDelayMSecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
        }
    }
}
