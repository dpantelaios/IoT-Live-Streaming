import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import model.AverageMeasurement;
import model.JsonSerde;
import model.Measurements;
import model.DiffMeasurements;
import model.HelpDate;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Date;


public class Consumer_java {
	public static void main(String[] args) {
		//create kafka consumer
		Properties properties = getConfig();
		Serde<Measurements> MeasurementSerde = new JsonSerde<>(Measurements.class);
        Serde<AverageMeasurement> AverageMeasurementSerde = new JsonSerde<>(AverageMeasurement.class);
        Serde<DiffMeasurements> DiffMeasurementsSerde = new JsonSerde<>(DiffMeasurements.class);
        Serde<HelpDate> HelpDateSerde = new JsonSerde<>(HelpDate.class);
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, Measurements> MeasurementsStream = streamsBuilder.stream("th1",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                // .peek(consumerRecord -> {System.out.println(consumerRecord.timestamp());})
                );
				// .map((sensor, measurement) -> KeyValue.pair(measurement.getdate(), measurement.getMeasurement()))
		
        // 15MINUTES SENSORS RAW
		MeasurementsStream
        // .to("RAW", Produced.with(Serdes.String(), MeasurementSerde));
        .groupByKey()
        .aggregate(()-> new HelpDate(0.0f, new Date()),
                (key, value, aggregate) -> {
                    aggregate.setValue(value.getValue());
                    aggregate.setProduceDate(value.timeDatehelp());
                    return aggregate;
                },
                Materialized.with(Serdes.String(), HelpDateSerde))
        .toStream()
        .map((k,v) -> KeyValue.pair(k, v))
        .to("RAW", Produced.with(Serdes.String(), HelpDateSerde));

        // AggDay[x]
        MeasurementsStream
        .peek((key, value) -> {System.out.println(key); System.out.println(value);})
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        // .windowedBy(TimeWindows.of(Duration.ofMillis(1000L)))
        // // .windowedBy(TimeWindows.of(Duration.ofSeconds(3, 0)))
        .aggregate(()-> new AverageMeasurement(0.0f, 0, 0.0f, new Date()),
                (key, value, aggregate) -> {
                    aggregate.setAddedValues(aggregate.getAddedValues()+value.getValue());
                    aggregate.setCount(aggregate.getCount()+1);
                    aggregate.setAvgMeasurement(aggregate.getAddedValues()/aggregate.getCount());
                    aggregate.setAggregationDate(value.withouttimeDate());
                    return aggregate;
                },
                Materialized.with(Serdes.String(), AverageMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek((key, value) -> {System.out.println(key.key()); System.out.println(value);})
        .map((k,v) -> KeyValue.pair(k.key(), v))
        .to("AGGREGATED", Produced.with(Serdes.String(), AverageMeasurementSerde));
        
        
        // DAILY SENSORS
        KStream<String, Measurements> DailyMeasurementsStream = streamsBuilder.stream("e_tot",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                );
		
        // DAILY RAW
		DailyMeasurementsStream
        // .peek((key, value) -> {System.out.println(key); System.out.println(value);})
        .to("DAILY_RAW", Produced.with(Serdes.String(), MeasurementSerde));
        
        // AggDayDiff[y]
        DailyMeasurementsStream
        .peek((key, value) -> {System.out.println(key); System.out.println(value);})
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofDays(2L)).advanceBy(Duration.ofDays(1L)))
        .aggregate(()-> new DiffMeasurements(0.0f, 0.0f, new Date()),
                (key, value, aggregate) -> {
                    aggregate.setCurrentValue(value.getValue() - aggregate.getPreviousValue());
                    aggregate.setPreviousValue(value.getValue());
                    aggregate.setDiffDate(value.getProduceDate());
                    return aggregate;
                },
                Materialized.with(Serdes.String(), DiffMeasurementsSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek((key, value) -> {System.out.println(key.key()); System.out.println(value);})
        .map((k,v) -> KeyValue.pair(k.key(), v))
        .to("AGGREGATED_DIFF", Produced.with(Serdes.String(), DiffMeasurementsSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        // Start the application
        kafkaStreams.start();
	}

	private static Properties getConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "LiveStreamingLayer");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29090");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		// properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }
}
