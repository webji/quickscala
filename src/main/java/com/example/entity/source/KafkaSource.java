package com.example.entity.source;

import com.example.util.Config;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder()
public class KafkaSource extends SourceBase implements DataSource {
    String address;
    String brokers;

    public String getAddress() {
        return this.address;
    }

    public String getBrokers() {
        return  this.brokers;
    }

    public static KafkaSource defaultSource() {
        KafkaSource kafkaSource = KafkaSource.builder()
                .address("localhost:2181")
                .brokers("localhost:9092")
                .build();
        kafkaSource.name = "Kafka Source";
        kafkaSource.desc = "A Sample Source for test";
        kafkaSource.type = SourceType.KAFKA_UNIVERSAL;
        return kafkaSource;
    }

//    @Override
    public String version() {
        switch (type) {
            case KAFKA_0_8:
                return "0.8";
            case KAFKA_0_9:
                return "0.9";
            case KAFKA_0_10:
                return "0.10";
            case KAFKA_1_0:
                return "1.0";
            case KAFKA_2_4_0:
                return "2.4.0";
            case KAFKA_2_11:
                return "2.11";
            case KAFKA_UNIVERSAL:
                return "universal";
            default:
                return "NA";
        }
    }

    public static KafkaSource fromMap(Map<String, Object> confMap) {
        Config conf = new Config(confMap);
        KafkaSource source = KafkaSource.builder()
                .address(conf.getString("address"))
                .brokers(conf.getString("brokers"))
                .build();
        source.id = conf.getInteger("id");
        source.name = conf.getString("name");
        source.desc = conf.getString("desc");
//        String type = conf.getString("type");
        source.type = SourceType.KAFKA_UNIVERSAL;
//        if (type == "KAFKA_UNIVERSAL") {
//
//        } else {
//            source.type = SourceType.NA;
//        }
        return source;
    }
}
