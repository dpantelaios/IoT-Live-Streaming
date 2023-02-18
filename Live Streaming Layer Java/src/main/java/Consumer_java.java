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

import model.JsonSerde;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Date;


public class Consumer_java {
	public static void main(String[] args) {
		//create kafka consumer
		Properties properties = getConfig();
		Serde<Measurements> MeasurementSerde = new JsonSerde<>(Measurements.class);
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, Measurements> bankBalancesStream = streamsBuilder.stream("th1",
				Consumed.with(Serdes.String(), MeasurementSerde));
				// .map((sensor, measurement) -> KeyValue.pair(measurement.getdate(), measurement.getMeasurement()))
				// .peek((key, value) -> {System.out.println(key); System.out.println(value);});
		
		bankBalancesStream.peek((key, value) -> {System.out.println(key); System.out.println(value);})
		.to("RAW", Produced.with(Serdes.String(), MeasurementSerde));
				// .toStream()
                // .groupByKey()
                // .aggregate(() -> new AggregatedMeasurement(new Date()),
                //         (key, value, aggregate) -> new AggregatedMeasurement().AddMeasurement(value).getTotalMeasurement(),
                //         Materialized.<Date , KeyValueStore<Bytes, byte[]>>as(BANK_BALANCES_STORE)
                //             .withKeySerde(Serdes.Date())
                //             .withValueSerde(Serdes.Int())
                // )
                // .toStream();
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        // Start the application
        kafkaStreams.start();
	}

	private static Properties getConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "th1");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29090");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }
}
