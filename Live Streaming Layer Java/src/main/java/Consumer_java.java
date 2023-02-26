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
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.StateStoreFactory;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.processor.*;

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
import model.TotalLeak;
import model.FilterLateEvents;
import model.FlaggedMeasurement;
import model.AcceptedMeasurement;
import model.RejectedMeasurement;
import model.TotalMovesMeasurement;

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
import java.util.*;



/* Peek command */
// .peek((key, value) -> {System.out.println(key.key()); System.out.println(value);})


public class Consumer_java {

    SummedMeasurement test;
    private void updateTest(SummedMeasurement value) {
        test = value;
    }

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
        Serde<FilterLateEvents> FilterLateEventsSerde = new JsonSerde<>(FilterLateEvents.class);
        Serde<AcceptedMeasurement> AcceptedMeasurementSerde = new JsonSerde<>(AcceptedMeasurement.class);
        Serde<RejectedMeasurement> RejectedMeasurementSerde = new JsonSerde<>(RejectedMeasurement.class);
        Serde<FlaggedMeasurement> FlaggedMeasurementSerde = new JsonSerde<>(FlaggedMeasurement.class);
        Serde<TotalMovesMeasurement> TotalMovesMeasurementSerde = new JsonSerde<>(TotalMovesMeasurement.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        DateFormat extractDateNoTime = new SimpleDateFormat("yyyy-MM-dd");
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
		KStream<String, Measurement> min15Stream = streamsBuilder.stream("th1",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                );
		
        /* filter late rejected events into filteredMin15Stream[1] */
        // KStream<String, FilterLateEvents>[] filteredMin15Stream = //need to add [] for branches
        // min15Stream
        // // .filter((key, value) -> sum15Energy.contains(key) || sum15Water.contains(key))
        // .groupByKey()
        // .aggregate(()-> new FilterLateEvents("false", new Date(1588338000), 0.0, new Date()),
        //         (key, value, aggregate) -> {
        //             aggregate.setIsLateEvent(aggregate.lateEvent(value.getProduceDate()));
        //             // aggregate.setMaxDate(new Date(Math.max(aggregate.getMaxDate().getTime(), value.getProduceDate().getTime())));
        //             aggregate.updateMaxDate(value.getProduceDate());
        //             aggregate.setFilteredValue(value.getValue());
        //             aggregate.setFilteredProduceDate(value.getProduceDate());
        //             return aggregate;
        //         },
        //         Materialized.with(Serdes.String(), FilterLateEventsSerde))
        // .toStream()
        // // .filter((key, value) -> value.getIsLateEvent() == "false")
        // // .map((k,v) -> KeyValue.pair(k, new AcceptedMeasurement(v.getFilteredValue(), v.getFilteredProduceDate())))
        // // .peek((key, value) -> {System.out.println("filtered_end"); System.out.println(key); System.out.println(value);})
        // .branch(
        //     (key, value) -> value.getIsLateEvent()=="false",
        //     (key, value) -> value.getIsLateEvent()=="true"
        //     )
        // ;
    
        KStream<String, AcceptedMeasurement> filteredMin15Stream =
        min15Stream
        .transform(()->new filteringTransformer(1589673600000L), "filter")
        .filter((key, value) -> value.getIsRejected()=="false")
        .map((key, value) -> KeyValue.pair(key, new AcceptedMeasurement(value.getValue(), value.getProduceDate())))
        ;

        KStream<String, RejectedMeasurement> RejectedMin15Stream =
        min15Stream
        .transform(()->new filteringTransformer(1589673600000L), "filter")
        .filter((key, value) -> value.getIsRejected()=="true")
        .map((key, value) -> KeyValue.pair(key, new RejectedMeasurement(value.getValue(), value.getProduceDate())))
        // .peek((key, value) -> {System.out.print("Rejected data: "); System.out.println(key); System.out.println(value);})
        ;

        //
        /* send raw data */
		// min15Stream
        filteredMin15Stream
        // .map((k,v) -> KeyValue.pair(k, new AcceptedMeasurement(v.getFilteredValue(), v.getFilteredProduceDate())))
        .map((key, value)->KeyValue.pair(key, new EnrichedMeasurement(value.getValue(), value.getProduceDate(), key.replaceAll("\"", ""))))
        .to("RAW", Produced.with(Serdes.String(), EnrichedMeasurementSerde));
		
        /* Calculate Daily Average AggDay[x] */
        // filteredMin15Stream[0]
        // .map((k,v) -> KeyValue.pair(k, new AcceptedMeasurement(v.getFilteredValue(), v.getFilteredProduceDate())))
        // min15Stream
        filteredMin15Stream
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
        
        
        // StoreSupplier filterStore = Stores.create("Filter").withKeys(Serdes.String()).withValues(FlaggedMeasurementSerde).persistent().build();
        

        // KStream<String, FlaggedMeasurement> lateRejectedEventsStream = 
        // min15Stream.transform(()->new filteringTransformer(1589673600000L), "filter")
        // // .map((k,v) -> KeyValue.pair(k, new RejectedMeasurement(v.getFilteredValue(), v.getFilteredProduceDate())))
        // .peek((key, value) -> {System.out.println("filtered_end"); System.out.println(key); System.out.println(value);})
        // // .to("RAW", Produced.with(Serdes.String(), AcceptedMeasurementSerde))
        // ;

        

        /* Calculate Daily Sum AggDay[X] */
        KStream<String, SummedMeasurement> summedMin15Stream = 
        // filteredMin15Stream[0]
        // .map((k,v) -> KeyValue.pair(k, new AcceptedMeasurement(v.getFilteredValue(), v.getFilteredProduceDate()))) //accepted inputs (not late rejected)
        // .peek((key, value) -> {System.out.print("Filtered data: "); System.out.println(key); System.out.println(value);})
        // min15Stream
        filteredMin15Stream

        .filter((key, value) -> sum15Energy.contains(key) || sum15Water.contains(key))
        .groupByKey(Serialized.with(Serdes.String(), AcceptedMeasurementSerde))
        // .groupByKey(Serialized.with(Serdes.String(), AcceptedMeasurementSerde))
        .windowedBy(TimeWindows.of(Duration.ofDays(1L)).grace(Duration.ofDays(2L)).until(259200000L))
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
        .map((k,v) -> KeyValue.pair(k.key(), new SummedMeasurement(Double.parseDouble(df.format(v.getSum())), v.getSumDate(), v.getSensorName())))
        .peek((key, value) -> {/*System.out.print("SUM: "); System.out.println(key);*/ System.out.println(value);})
        ;

        summedMin15Stream
        .to("AGGREGATED", Produced.with(Serdes.String(), SummedMeasurementSerde));
        
        /* Sum of total daily device energy consumption (e.g. hvac1+hvac2+miac1...) */
        KStream<String, LeakSum> devicesEnergyStream =
        summedMin15Stream
        .filter((key, value) -> sum15Energy.contains(key)) //we don't want w1 in our sum
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
        .filter((key, value)->value.getLeakSumCount()>=3) //change number to sum15Energy.size
        .map((k,v) -> KeyValue.pair(k.key(), new LeakSum(Double.parseDouble(df.format(v.getLeakSum())), v.getLeakSumCount(), v.getLeakSumDate())))
        ;

        /* Sum of total daily water consumption (in our case just w1)*/
        KStream<String, LeakSum> devicesWaterStream = 
        summedMin15Stream
        .filter((key, value) -> sum15Water.contains(key))
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
        .filter((key, value)->value.getLeakSumCount()>=1) //change number to sum15Water.size
        .map((k,v) -> KeyValue.pair(k.key(), new LeakSum(Double.parseDouble(df.format(v.getLeakSum())), v.getLeakSumCount(), v.getLeakSumDate())))
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
        KStream<String, DiffMeasurement> dailyDiffStream = 
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
        .map((k,v) -> KeyValue.pair(k.key(), v));

        /* send to AggDayDiff Stream */
        dailyDiffStream
        .to("AGGREGATED_DIFF", Produced.with(Serdes.String(), DiffMeasurementSerde));

        /* Stream contains ENERGY only (not water) total daily diff */
        KStream<String, DiffMeasurement> dailyDiffEnergyTotalStream = 
        dailyDiffStream
        .filter((key, value) -> daily15Energy.contains(key))
        .map((key, value) -> KeyValue.pair(jsonDateFormat.format(value.getDiffDate()), value));

        /* Stream contains WATER only (not energy) total daily diff */
        KStream<String, DiffMeasurement> dailyDiffWaterTotalStream = 
        dailyDiffStream
        .filter((key, value) -> daily15Water.contains(key))
        .map((key, value) -> KeyValue.pair(jsonDateFormat.format(value.getDiffDate()), value));

        /* Calculate leakage by joining dailyDiffEnergyTotalStream with devicesEnergyStream and subtracting the values */
        ValueJoiner<DiffMeasurement, LeakSum, TotalLeak> leakJoiner = (diffEnergy, summedEnergy) ->
            new TotalLeak(diffEnergy.getCurrentValue(), summedEnergy.getLeakSum(), diffEnergy.getDiffDate());

        KStream<String, TotalLeak> EnergyLeakStream = dailyDiffEnergyTotalStream.join(devicesEnergyStream, leakJoiner, JoinWindows.of(Duration.ofDays(1L)), StreamJoined.with(Serdes.String(), DiffMeasurementSerde, LeakSumSerde));

        EnergyLeakStream
        .peek((key, value) -> {System.out.println("energy_joined_start"); System.out.println(key); System.out.println(value);});
        
        KStream<String, TotalLeak> WaterLeakStream = dailyDiffWaterTotalStream.join(devicesWaterStream, leakJoiner, JoinWindows.of(Duration.ofDays(1L)), StreamJoined.with(Serdes.String(), DiffMeasurementSerde, LeakSumSerde));

        WaterLeakStream
        .peek((key, value) -> {System.out.println("water_joined_start"); System.out.println(key); System.out.println(value);});
        
        //Consume Move Detection Measurements
        KStream<String, Measurement> moveDetectionStream = streamsBuilder.stream("moveDetection",
				Consumed.with(Serdes.String(), MeasurementSerde)
                .withTimestampExtractor(new MeasurementTimeExtractor())
                );
        
        KStream<String, TotalMovesMeasurement> totalMoveDetectionStream =
        moveDetectionStream
        .transform(()->new totalMovesTransformer(), "totalMoves")
        // .filter((key, value) -> value.getIsRejected()=="false")
        // .map((key, value) -> KeyValue.pair(key, new AcceptedMeasurement(value.getValue(), value.getProduceDate())))
        .peek((key, value) -> {System.out.println("total_moves_end"); System.out.println(key); System.out.println(value);});

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
