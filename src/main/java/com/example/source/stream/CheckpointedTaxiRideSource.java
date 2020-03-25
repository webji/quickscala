package com.example.source.stream;

import com.example.entity.TaxiRide;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class CheckpointedTaxiRideSource implements SourceFunction<TaxiRide>, ListCheckpointed<Long> {

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    private long eventCount = 0;

    public CheckpointedTaxiRideSource(String dataFilePath) { this(dataFilePath, 1); }

    public CheckpointedTaxiRideSource(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(eventCount);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        for (Long s : state) {
            this.eventCount = s;
        }
    }

    @Override
    public void run(SourceContext<TaxiRide> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();

        gzipStream = new GZIPInputStream(new FileInputStream((dataFilePath)));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

        Long prevRideTime = null;

        String line;
        long count = 0;

        while (count < eventCount && reader.ready() && (line = reader.readLine()) != null) {
            count ++;
            TaxiRide ride = TaxiRide.fromString(line);
            prevRideTime = ride.getEventTime();
        }

        while (reader.ready() && (line = reader.readLine()) != null) {
            TaxiRide ride = TaxiRide.fromString(line);
            long rideTime = ride.getEventTime();

            if (prevRideTime != null) {
                long diff = (rideTime - prevRideTime) / servingSpeed;
                Thread.sleep(diff);
            }

            synchronized (lock) {
                eventCount++;
                ctx.collectWithTimestamp(ride, rideTime);
                ctx.emitWatermark(new Watermark(rideTime - 1));
            }

            prevRideTime = rideTime;
        }

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;
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
