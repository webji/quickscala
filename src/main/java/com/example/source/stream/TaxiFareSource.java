package com.example.source.stream;

import com.example.entity.TaxiFare;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;

public class TaxiFareSource implements SourceFunction<TaxiFare> {

    private final int maxDelayMsecs;
    private final int watermarkDelaySecs;

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    public TaxiFareSource(String dataFilePath) { this(dataFilePath, 0, 1); }
    public TaxiFareSource(String dataFilePath, int servingSpeedFactor) { this(dataFilePath, 0, servingSpeedFactor); }
    public TaxiFareSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if (maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelaySecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
    }


    @Override
    public void run(SourceContext<TaxiFare> ctx) throws Exception {
        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

        generateUnorderedStream(ctx);

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;

    }

    private void generateUnorderedStream(SourceContext<TaxiFare> sourceContext) throws Exception {
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
        TaxiFare fare;

        if (reader.ready() && (line = reader.readLine()) != null) {
            fare = TaxiFare.fromString(line);

            dataStartTime = fare.getEventTime();

            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            emitSchedule.add(new Tuple2<>(delayedEventTime, fare));
            long watermarkTime = dataStartTime + watermarkDelaySecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            emitSchedule.add(new Tuple2<>(watermarkTime, nextWatermark));
        } else {
            return;
        }

        if (reader.ready() && (line = reader.readLine()) != null) {
            fare = TaxiFare.fromString(line);
        }

        while (emitSchedule.size() > 0 || reader.ready()) {
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long rideEventTime = fare != null ? fare.getEventTime() : -1;
            while (
                    fare != null && (emitSchedule.isEmpty() || rideEventTime < curNextDelayedEventTime + maxDelayMsecs)
            ) {
                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<>(delayedEventTime, fare));

                if (reader.ready() && (line = reader.readLine()) != null) {
                    fare = TaxiFare.fromString(line);
                    rideEventTime = fare.getEventTime();
                } else {
                    fare = null;
                    rideEventTime = -1;
                }
            }

            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;

            long now = Calendar.getInstance().getTimeInMillis();
            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            long waitTime = servingTime - now;

            Thread.sleep( (waitTime > 0) ? waitTime : 0);

            if (head.f1 instanceof TaxiFare) {
                TaxiFare emitFare = (TaxiFare)head.f1;
                sourceContext.collectWithTimestamp(emitFare, emitFare.getEventTime());
            } else if (head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark)head.f1;
                sourceContext.emitWatermark(emitWatermark);
                long watermarkTime = delayedEventTime + watermarkDelaySecs;
                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                emitSchedule.add(new Tuple2<>(watermarkTime, nextWatermark));
            }
        }
    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while (delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {

    }
}
