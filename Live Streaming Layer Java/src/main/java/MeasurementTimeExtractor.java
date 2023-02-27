import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import model.Measurement;

public class MeasurementTimeExtractor implements TimestampExtractor{
    
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partirionTime) {
        var measurementInfo = (Measurement) record.value();
        return Optional.ofNullable(measurementInfo.getProduceDate())
                .map(it -> it.toInstant().toEpochMilli())
                .orElse(partirionTime);
    }
}
