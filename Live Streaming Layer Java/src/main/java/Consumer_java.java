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
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, Measurements> MeasurementsStream = streamsBuilder.stream("th1",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                // .peek(consumerRecord -> {System.out.println(consumerRecord.timestamp());})
                );
				// .map((sensor, measurement) -> KeyValue.pair(measurement.getdate(), measurement.getMeasurement()))
				// .peek((key, value) -> {System.out.println(key); System.out.println(value);});
		
		// MeasurementsStream
        // .peek((key, value) -> {System.out.println(key); System.out.println(value);})
        // .filter((key, value) -> key.toString()=="th1")
        // .peek((key, value) -> {System.out.println(key);});
        // .peek((key, value) -> {System.out.println(key); System.out.println(value);});
		// .to("RAW", Produced.with(Serdes.String(), MeasurementSerde));

        MeasurementsStream
        .peek((key, value) -> {System.out.println("bla0"); System.out.println(value);})
        .groupByKey()
        // .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        // .windowedBy(TimeWindows.of(Duration.ofMillis(1000L)))
        .windowedBy(TimeWindows.of(Duration.ofMinutes(300L)))
        // // .windowedBy(TimeWindows.of(Duration.ofSeconds(3, 0)))
        // //.count()
        .aggregate(()-> new AverageMeasurement(0.0f, 0, 0.0f),
                (key, value, aggregate) -> {
                    aggregate.setAddedValues(aggregate.getAddedValues()+value.getValue());
                    aggregate.setCount(aggregate.getCount()+1);
                    aggregate.setAvgMeasurement(aggregate.getAddedValues()/aggregate.getCount());
                    return aggregate;
                },
                Materialized.with(Serdes.String(), AverageMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek((key, value) -> {System.out.println(key.key()); System.out.println(value);})
        .map((k,v) -> KeyValue.pair(k.key(), v))
        .to("RAW", Produced.with(Serdes.String(), AverageMeasurementSerde));

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
