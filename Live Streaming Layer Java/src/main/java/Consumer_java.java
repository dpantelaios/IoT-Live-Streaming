import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.errors.*;

import model.AverageMeasurement;
import model.JsonSerde;
import model.MaxMeasurement;
import model.Measurement;
import model.DiffMeasurement;
import model.SummedMeasurement;
import model.EnrichedMeasurement;
import model.LeakSum;
import model.TotalLeak;
import model.FlaggedMeasurement;
import model.AcceptedMeasurement;
import model.RejectedMeasurement;
import model.TotalMovesMeasurement;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.Date;
import java.util.HashSet;
import java.util.Arrays;
import java.text.DecimalFormat;


/* Peek command */
// .peek((key, value) -> {System.out.println(key.key()); System.out.println(value);})


public class Consumer_java {

	public static void main(String[] args) {

        Set<String> avg15 = new HashSet<String>(Arrays.asList("\"th1\"", "\"th2\""));
        Set<String> sum15Energy = new HashSet<String>(Arrays.asList("\"hvac1\"", "\"hvac2\"", "\"miac1\"","\"miac2\""));
        Set<String> sum15Water = new HashSet<String>(Arrays.asList("\"w1\""));
        Set<String> daily15Energy = new HashSet<String>(Arrays.asList("\"etot\""));
        Set<String> daily15Water = new HashSet<String>(Arrays.asList("\"wtot\""));

        
		//create kafka consumer 
		Properties properties = getConfig();
		Serde<Measurement> MeasurementSerde = new JsonSerde<>(Measurement.class);
        Serde<AverageMeasurement> AverageMeasurementSerde = new JsonSerde<>(AverageMeasurement.class);
        Serde<DiffMeasurement> DiffMeasurementSerde = new JsonSerde<>(DiffMeasurement.class);
        Serde<SummedMeasurement> SummedMeasurementSerde = new JsonSerde<>(SummedMeasurement.class);
        Serde<MaxMeasurement> MaxMeasurementSerde = new JsonSerde<>(MaxMeasurement.class);
        Serde<EnrichedMeasurement> EnrichedMeasurementSerde = new JsonSerde<>(EnrichedMeasurement.class);
        Serde<LeakSum> LeakSumSerde = new JsonSerde<>(LeakSum.class);
        Serde<TotalLeak> TotalLeakSerde = new JsonSerde<>(TotalLeak.class);
        Serde<AcceptedMeasurement> AcceptedMeasurementSerde = new JsonSerde<>(AcceptedMeasurement.class);
        Serde<RejectedMeasurement> RejectedMeasurementSerde = new JsonSerde<>(RejectedMeasurement.class);
        Serde<FlaggedMeasurement> FlaggedMeasurementSerde = new JsonSerde<>(FlaggedMeasurement.class);
        Serde<TotalMovesMeasurement> TotalMovesMeasurementSerde = new JsonSerde<>(TotalMovesMeasurement.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        SimpleDateFormat jsonDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final DecimalFormat df = new DecimalFormat("0.00");

        StoreBuilder<KeyValueStore<String, Measurement>> filterStoreSupplier =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("filter"),
                Serdes.String(),
                MeasurementSerde
                );
        KeyValueStore<String, Measurement> filterStore = filterStoreSupplier.build();

        StoreBuilder<KeyValueStore<String, Measurement>> totalMovesStoreSupplier =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("totalMoves"),
                Serdes.String(),
                MeasurementSerde
                );
        KeyValueStore<String, Measurement> totalMovesStore = totalMovesStoreSupplier.build();

        streamsBuilder.addStateStore(filterStoreSupplier);
        streamsBuilder.addStateStore(totalMovesStoreSupplier);

    
        /* 15 MIN DATA */

        /* stream to consume from 15min average topics and extract proper timestamps */
		KStream<String, Measurement> min15Stream = streamsBuilder.stream("min15",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                );
	
    
        KStream<String, AcceptedMeasurement> filteredMin15Stream =
        min15Stream
        .transform(()->new FilteringTransformer(1589673600000L), "filter")
        .filter((key, value) -> value.getIsRejected()=="false")
        .map((key, value) -> KeyValue.pair(key, new AcceptedMeasurement(value.getValue(), value.getProduceDate())))
        ;

        KStream<String, RejectedMeasurement> RejectedMin15Stream =
        min15Stream
        .transform(()->new FilteringTransformer(1589673600000L), "filter")
        .filter((key, value) -> value.getIsRejected()=="true")
        .map((key, value) -> KeyValue.pair(key, new RejectedMeasurement(value.getValue(), value.getProduceDate())))
        ;
        RejectedMin15Stream
        .map((key, value)->KeyValue.pair(key, new EnrichedMeasurement(value.getValue(), value.getProduceDate(), "LateRejected_" + key.replaceAll("\"", ""))))
        .to("lateRejected", Produced.with(Serdes.String(), EnrichedMeasurementSerde));

        /* send raw data */
        filteredMin15Stream
        .map((key, value)->KeyValue.pair(key, new EnrichedMeasurement(value.getValue(), value.getProduceDate(), key.replaceAll("\"", ""))))
        .to("raw", Produced.with(Serdes.String(), EnrichedMeasurementSerde));
		
        /* Calculate Daily Average AggDay[x] and send it to appropriate topic*/
        filteredMin15Stream
        .filter((key, value) -> avg15.contains(key)) // filter data in order to keep only 15 minute sensors' data with aggregation function average
        .groupByKey(Grouped.with(Serdes.String(), AcceptedMeasurementSerde)) // group data by key=SensorName and specify datatypes for serialization and deserialization of input data
        .windowedBy(TimeWindows.of(Duration.ofDays(1L))) // use window of 1 day duration to capture separately each day's data 
        .aggregate(()-> new AverageMeasurement(0.0, 0, 0.0, new Date(), new String()),  // initialize count to 0 and totalsum to 0.0
                (key, value, aggregate) -> {
                    aggregate.setAddedValues(aggregate.getAddedValues()+value.getValue());  //sum values of daily data
                    aggregate.setCount(aggregate.getCount()+1); //count number of sensor measurements through the day
                    aggregate.setAvgMeasurement(aggregate.getAddedValues()/aggregate.getCount()); // divide summed daily values of sensor with the total number of measurements to compute daily average
                    aggregate.setAggregationDate(value.withouttimeDate()); //keep Date and set time to 00:00:00
                    aggregate.setSensorName(key.replaceAll("\"", "")); // save sensor's name
                    return aggregate;
                },
                Materialized.with(Serdes.String(), AverageMeasurementSerde)) // specify datatypes for serialization and deserialization of produced data
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())) // output only the final result of the window
        .toStream() // convert KTable back to KStream
        .map((k,v) -> KeyValue.pair(k.key(), new AverageMeasurement(Double.parseDouble(df.format(v.getAddedValues())), v.getCount(), Double.parseDouble(df.format(v.getAvgMeasurement())), v.getAggregationDate(), v.getSensorName()))) // restore initial key format which has been changed by the window implementation
        .to("aggDay15min", Produced.with(Serdes.String(), AverageMeasurementSerde)); //specify serialization data type and send to topic aggDay15min
       
    
        /* Calculate Daily Sum AggDay[X] and save the stream to use for further calculations*/
        KStream<String, SummedMeasurement> summedMin15Stream = 
        filteredMin15Stream
        .filter((key, value) -> sum15Energy.contains(key) || sum15Water.contains(key))
        .groupByKey(Grouped.with(Serdes.String(), AcceptedMeasurementSerde))
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)).grace(Duration.ofDays(2L)).until(259200000L))
        .aggregate(()-> new SummedMeasurement(0.0, new Date(), new String()),
                (key, value, aggregate) -> {
                    aggregate.setSum(aggregate.getSum() + value.getValue());
                    aggregate.setAggregationDate(value.withouttimeDate());
                    aggregate.setSensorName(key.replaceAll("\"", ""));
                    return aggregate;
                },
                Materialized.with(Serdes.String(), SummedMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((k,v) -> KeyValue.pair(k.key(), new SummedMeasurement(Double.parseDouble(df.format(v.getSum())), v.getAggregationDate(), v.getSensorName())))
        ;

        /* send sum to AGGREGATED topic */
        summedMin15Stream
        .to("aggDay15min", Produced.with(Serdes.String(), SummedMeasurementSerde));
        
        /* Sum of total daily device energy consumption (e.g. hvac1+hvac2+miac1...) */
        KStream<String, LeakSum> devicesEnergyStream =
        summedMin15Stream
        .filter((key, value) -> sum15Energy.contains(key)) //we don't want w1 in our sum
        .groupBy((key, value) -> jsonDateFormat.format(value.getAggregationDate()), Grouped.with(Serdes.String(), SummedMeasurementSerde))
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        .aggregate(() -> new LeakSum(0.0, 0, new Date()),
                (key, value, aggregate) -> {
                    aggregate.setLeakSum(aggregate.getLeakSum() + value.getSum());
                    aggregate.setLeakSumCount(aggregate.getLeakSumCount()+1);
                    aggregate.setLeakSumDate(value.getAggregationDate());
                    return aggregate;
                },
                Materialized.with(Serdes.String(), LeakSumSerde))
        .toStream()
        .filter((key, value)->value.getLeakSumCount()>=sum15Energy.size()) 
        .map((k,v) -> KeyValue.pair(k.key(), new LeakSum(Double.parseDouble(df.format(v.getLeakSum())), v.getLeakSumCount(), v.getLeakSumDate())))
        ;

        /* Sum of total daily water consumption (in our case just w1)*/
        KStream<String, LeakSum> devicesWaterStream = 
        summedMin15Stream
        .filter((key, value) -> sum15Water.contains(key))
        .groupBy((key, value) -> jsonDateFormat.format(value.getAggregationDate()), Grouped.with(Serdes.String(), SummedMeasurementSerde))
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        .aggregate(() -> new LeakSum(0.0, 0, new Date()),
                (key, value, aggregate) -> {
                    aggregate.setLeakSum(aggregate.getLeakSum() + value.getSum());
                    aggregate.setLeakSumCount(aggregate.getLeakSumCount()+1);
                    aggregate.setLeakSumDate(value.getAggregationDate());
                    return aggregate;
                },
                Materialized.with(Serdes.String(), LeakSumSerde))
        .toStream()
        .filter((key, value)->value.getLeakSumCount()>=sum15Water.size()) 
        .map((k,v) -> KeyValue.pair(k.key(), new LeakSum(Double.parseDouble(df.format(v.getLeakSum())), v.getLeakSumCount(), v.getLeakSumDate())))
        ;

        
        /* DAILY DATA */
        KStream<String, Measurement> dailyStream = streamsBuilder.stream("day",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                );
		
        /* send raw data */
		dailyStream
        .map((key, value)->KeyValue.pair(key, new EnrichedMeasurement(value.getValue(), value.getProduceDate(), key.replaceAll("\"", ""))))
        .to("raw", Produced.with(Serdes.String(), EnrichedMeasurementSerde));
        
        /* Calculate Daily Max AggDay[X] and save stream for further calculations (in our case -1 data per day- Max=Raw) */
        KStream<String, MaxMeasurement> dailyMaxStream = 
        dailyStream
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)))
        .aggregate(()-> new MaxMeasurement(0.0, new Date(), new String()),
                (key, value, aggregate) -> {
                    aggregate.setMaxValue(Math.max(aggregate.getMaxValue(), value.getValue()));
                    aggregate.setMaxDate(value.getProduceDate());
                    aggregate.setSensorName(key.replaceAll("\"", ""));
                    return aggregate;
                },
                Materialized.with(Serdes.String(), MaxMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((k,v) -> KeyValue.pair(k.key(), v));

        /* Send Daily aggregated data */
        dailyMaxStream
        .to("aggDayDaily", Produced.with(Serdes.String(), MaxMeasurementSerde));

        // AggDayDiff[y] (currDayMax - prevDateMax)
        KStream<String, DiffMeasurement> dailyDiffStream = 
        dailyMaxStream
        .groupByKey(Grouped.with(Serdes.String(), MaxMeasurementSerde))
        .windowedBy(TimeWindows.of(Duration.ofDays(2L)).advanceBy(Duration.ofDays(1L)))
        .aggregate(()-> new DiffMeasurement(0.0, 0.0, new Date(), new String()),
                (key, value, aggregate) -> {
                    aggregate.setCurrentValue(value.getMaxValue() - aggregate.getPreviousValue());
                    aggregate.setPreviousValue(value.getMaxValue());
                    aggregate.setDiffDate(value.getMaxDate());
                    aggregate.setSensorName(key.replaceAll("\"", ""));
                    return aggregate;
                },
                Materialized.with(Serdes.String(), DiffMeasurementSerde))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((k,v) -> KeyValue.pair(k.key(), v));

        /* send to AggDayDiff Stream */
        dailyDiffStream
        .to("aggDayDiff", Produced.with(Serdes.String(), DiffMeasurementSerde));

        /* Stream contains ENERGY only (not water) total daily diff */
        KStream<String, DiffMeasurement> dailyDiffEnergyTotalStream = 
        dailyDiffStream
        .filter((key, value) -> daily15Energy.contains(key))
        .map((key, value) -> KeyValue.pair(jsonDateFormat.format(value.getDiffDate()), value))
        ;

        /* Stream contains WATER only (not energy) total daily diff */
        KStream<String, DiffMeasurement> dailyDiffWaterTotalStream = 
        dailyDiffStream
        .filter((key, value) -> daily15Water.contains(key))
        .map((key, value) -> KeyValue.pair(jsonDateFormat.format(value.getDiffDate()), value));

        /* Calculate leakage by joining dailyDiffEnergyTotalStream with devicesEnergyStream and subtracting the values */
        ValueJoiner<DiffMeasurement, LeakSum, TotalLeak> leakJoiner = (diffEnergy, summedEnergy) ->
            new TotalLeak(diffEnergy.getCurrentValue(), summedEnergy.getLeakSum(), diffEnergy.getDiffDate(), new String());

        KStream<String, TotalLeak> EnergyLeakStream = dailyDiffEnergyTotalStream.join(devicesEnergyStream, leakJoiner, JoinWindows.of(Duration.ofDays(1L)), StreamJoined.with(Serdes.String(), DiffMeasurementSerde, LeakSumSerde));

        EnergyLeakStream
        .map((key, value) -> KeyValue.pair("Energy", new TotalLeak(value.getLeak(), value.getLeakDate(), "Energy")))
        .to("leaks", Produced.with(Serdes.String(), TotalLeakSerde));
        
        KStream<String, TotalLeak> WaterLeakStream = dailyDiffWaterTotalStream.join(devicesWaterStream, leakJoiner, JoinWindows.of(Duration.ofDays(1L)), StreamJoined.with(Serdes.String(), DiffMeasurementSerde, LeakSumSerde));

        WaterLeakStream
        .map((key, value) -> KeyValue.pair("Water", new TotalLeak(value.getLeak(), value.getLeakDate(), "Water")))
        .to("leaks", Produced.with(Serdes.String(), TotalLeakSerde));
        
        /* Consume Move Detection Measurements */
        KStream<String, Measurement> moveDetectionStream = streamsBuilder.stream("movementSensor",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                );
        
        /* Send raw move detection data */
        moveDetectionStream
        .map((key, value)->KeyValue.pair(key, new EnrichedMeasurement(value.getValue(), value.getProduceDate(), key.replaceAll("\"", ""))))
        .to("raw", Produced.with(Serdes.String(), EnrichedMeasurementSerde));
        
        /* Count total move detections and send sum up to date */
        moveDetectionStream
        .transform(()->new TotalMovesTransformer(), "totalMoves")
        .to("totalMovements", Produced.with(Serdes.String(), TotalMovesMeasurementSerde))
        ;

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
        // properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "3");
        // properties.put(
        // StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        // LogAndContinueExceptionHandler.class.getName()
        // );
		// properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }
}
