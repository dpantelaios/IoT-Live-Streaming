import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
// import java.util.Optional;
import java.time.Duration;


import model.Measurements;

public class MeasurementTimeExtractor implements TimestampExtractor{
    
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partirionTime) {
        var measurementInfo = (Measurements) record.value();
        // System.out.println(measurementInfo.getProduceDate().toInstant().toEpochMilli());
        // System.out.println(Duration.ofMillis(3000));
        // return measurementInfo.getProduceDate().toInstant().toEpochMilli(); // must use Optional if time can be null
        return Optional.ofNullable(measurementInfo.getProduceDate())
                .map(it -> it.toInstant().toEpochMilli())
                .orElse(partirionTime);
    }
}
