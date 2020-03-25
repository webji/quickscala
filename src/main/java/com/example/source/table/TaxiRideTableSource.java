package com.example.source.table;

import com.example.entity.TaxiRide;
import com.example.source.stream.TaxiRideSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TaxiRideTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

    private final TaxiRideSource taxiRideSource;

    public TaxiRideTableSource(String dataFilePath) { this.taxiRideSource = new TaxiRideSource(dataFilePath); }
    public TaxiRideTableSource(String dataFilePath, int servingSpeedFactor) { this.taxiRideSource = new TaxiRideSource(dataFilePath, servingSpeedFactor); }
    public TaxiRideTableSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        this.taxiRideSource = new TaxiRideSource(dataFilePath, maxEventDelaySecs, servingSpeedFactor);
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor("eventTime", new StreamRecordTimestamp(), new PreserveWatermarks());
        return Collections.singletonList(descriptor);
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        return streamExecutionEnvironment
                .addSource(this.taxiRideSource)
                .map(new TaxiRideToRow()).returns(getReturnType());
    }

    @Override
    public TableSchema getTableSchema() {
        TypeInformation<?>[] types = new TypeInformation[] {
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.BOOLEAN,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.SHORT,
                Types.SQL_TIMESTAMP
        };

        String[] names = new String[]{
                "rideId",
                "taxiId",
                "driverId",
                "isStart",
                "startLon",
                "startLat",
                "endLon",
                "endLat",
                "passengerCnt",
                "eventTime"
        };

        return new TableSchema(names, types);
    }

    public static class TaxiRideToRow implements MapFunction<TaxiRide, Row> {

        @Override
        public Row map(TaxiRide value) throws Exception {
            return Row.of(
                    value.rideId,
                    value.taxiId,
                    value.driverId,
                    value.isStart,
                    value.startLon,
                    value.startLat,
                    value.endLon,
                    value.endLat,
                    value.passengerCnt);
        }
    }
}
