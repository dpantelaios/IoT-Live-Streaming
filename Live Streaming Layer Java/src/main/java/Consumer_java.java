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
import model.MaxMeasurement;
import model.Measurement;
import model.DiffMeasurement;
import model.SummedMeasurement;
import model.EnrichedMeasurement;
import model.LeakSum;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.Date;
import java.util.HashSet;
import java.util.Arrays;
import java.text.DecimalFormat;


/* Peek command */
// .peek((key, value) -> {System.out.println(key.key()); System.out.println(value);})


public class Consumer_java {

    SummedMeasurement test;
    private void updateTest(SummedMeasurement value) {
        test = value;
    }

	public static void main(String[] args) {

        Set<String> avg15 = new HashSet<String>(Arrays.asList("\"th1\"", "\"th2\""));
        Set<String> sum15 = new HashSet<String>(Arrays.asList("\"hvac1\"", "\"hvac2\"", "\"miac1\"","\"miac2\"","\"etot\""));

		//create kafka consumer 
		Properties properties = getConfig();
		Serde<Measurement> MeasurementSerde = new JsonSerde<>(Measurement.class);
        Serde<AverageMeasurement> AverageMeasurementSerde = new JsonSerde<>(AverageMeasurement.class);
        Serde<DiffMeasurement> DiffMeasurementSerde = new JsonSerde<>(DiffMeasurement.class);
        Serde<SummedMeasurement> SummedMeasurementSerde = new JsonSerde<>(SummedMeasurement.class);
        Serde<MaxMeasurement> MaxMeasurementSerde = new JsonSerde<>(MaxMeasurement.class);
        Serde<EnrichedMeasurement> EnrichedMeasurementSerde = new JsonSerde<>(EnrichedMeasurement.class);
        Serde<LeakSum> LeakSumSerde = new JsonSerde<>(LeakSum.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        DateFormat extractDateNoTime = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat jsonDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final DecimalFormat df = new DecimalFormat("0.00");


        /* 15 MIN DATA */

        /* stream to consume from 15min average topics and extract proper timestamps */
		KStream<String, Measurement> min15Stream = streamsBuilder.stream("th1",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                );
		
        /* send raw data */
		min15Stream
        .map((key, value)->KeyValue.pair(key, new EnrichedMeasurement(value.getValue(), value.getProduceDate(), key.replaceAll("\"", ""))))
        .to("RAW", Produced.with(Serdes.String(), EnrichedMeasurementSerde));
		
        /* Calculate Daily Average AggDay[x] */
        min15Stream
        .filter((key, value) -> avg15.contains(key))
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        .aggregate(()-> new AverageMeasurement(0.0, 0, 0.0, new Date(), new String()),
                (key, value, aggregate) -> {
                    aggregate.setAddedValues(aggregate.getAddedValues()+value.getValue());
                    aggregate.setCount(aggregate.getCount()+1);
                    aggregate.setAvgMeasurement(aggregate.getAddedValues()/aggregate.getCount());
                    aggregate.setAggregationDate(value.withouttimeDate());
                    aggregate.setSensorName(key.replaceAll("\"", ""));
                    return aggregate;
                },
                Materialized.with(Serdes.String(), AverageMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((k,v) -> KeyValue.pair(k.key(), v))
        .to("AGGREGATED", Produced.with(Serdes.String(), AverageMeasurementSerde));

        /* Calculate Daily Sum AggDay[X] */
        KStream<String, SummedMeasurement> summedMin15Stream = 
        min15Stream
        .filter((key, value) -> sum15.contains(key))
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        .aggregate(()-> new SummedMeasurement(0.0, new Date(), new String()),
                (key, value, aggregate) -> {
                    aggregate.setSum(aggregate.getSum() + value.getValue());
                    aggregate.setSumDate(value.withouttimeDate());
                    aggregate.setSensorName(key.replaceAll("\"", ""));
                    return aggregate;
                },
                Materialized.with(Serdes.String(), SummedMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        // .foreach((key, value) -> updateTest(value));
        .map((k,v) -> KeyValue.pair(k.key(), v))
        .map((k,v) -> KeyValue.pair(k, new SummedMeasurement(Double.parseDouble(df.format(v.getSum())), v.getSumDate(), v.getSensorName())));

        summedMin15Stream
        .to("AGGREGATED", Produced.with(Serdes.String(), SummedMeasurementSerde));
        
        summedMin15Stream
        // .peek((key, value) -> {System.out.println("start of stream"); System.out.println(key); System.out.println(value);})
        .groupBy((key, value) -> jsonDateFormat.format(value.getSumDate()), Grouped.with(Serdes.String(), SummedMeasurementSerde))
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        .aggregate(() -> new LeakSum(0.0, 0, new Date()),
                (key, value, aggregate) -> {
                    aggregate.setLeakSum(aggregate.getLeakSum() + value.getSum());
                    aggregate.setLeakSumCount(aggregate.getLeakSumCount()+1);
                    aggregate.setLeakSumDate(value.getSumDate());
                    return aggregate;
                },
                Materialized.with(Serdes.String(), LeakSumSerde))
        .toStream()
        .filter((key, value)->value.getLeakSumCount()>=3) //change number to sum15.size
        .map((k,v) -> KeyValue.pair(k, new LeakSum(Double.parseDouble(df.format(v.getLeakSum())), v.getLeakSumCount(), v.getLeakSumDate())))
        .peek((key, value) -> {System.out.println("end of stream"); System.out.println(key); System.out.println(value);})
        ;
        
        /* DAILY DATA */
        KStream<String, Measurement> dailyMaxStream = streamsBuilder.stream("etot",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                );
		
        /* send raw data */
		dailyMaxStream
        .map((key, value)->KeyValue.pair(key, new EnrichedMeasurement(value.getValue(), value.getProduceDate(), key.replaceAll("\"", ""))))
        .to("DAILY_RAW", Produced.with(Serdes.String(), EnrichedMeasurementSerde));
        
        /* Calculate Daily Max AggDay[X] */
        dailyMaxStream
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        .aggregate(()-> new MaxMeasurement(0.0, new Date(), new String()),
                (key, value, aggregate) -> {
                    aggregate.setMaxValue(Math.max(aggregate.getMaxValue(), value.getValue()));
                    aggregate.setMaxDate(value.getProduceDate());
                    // aggregate.setSensorName(key.substring(1, key.length()-1));
                    aggregate.setSensorName(key.replaceAll("\"", ""));
                    return aggregate;
                },
                Materialized.with(Serdes.String(), MaxMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((k,v) -> KeyValue.pair(k.key(), v))
        .to("AGGREGATED_DIFF", Produced.with(Serdes.String(), MaxMeasurementSerde));

        // AggDayDiff[y] (currDay - prevDate)
        dailyMaxStream
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofDays(2L)).advanceBy(Duration.ofDays(1L)))
        .aggregate(()-> new DiffMeasurement(0.0, 0.0, new Date(), new String()),
                (key, value, aggregate) -> {
                    aggregate.setCurrentValue(value.getValue() - aggregate.getPreviousValue());
                    aggregate.setPreviousValue(value.getValue());
                    aggregate.setDiffDate(value.getProduceDate());
                    aggregate.setSensorName(key.replaceAll("\"", ""));
                    return aggregate;
                },
                Materialized.with(Serdes.String(), DiffMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((k,v) -> KeyValue.pair(k.key(), v))
        .to("AGGREGATED_DIFF", Produced.with(Serdes.String(), DiffMeasurementSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.cleanUp();

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
