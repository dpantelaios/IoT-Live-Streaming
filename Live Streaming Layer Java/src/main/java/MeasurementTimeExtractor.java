import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
// import java.util.Optional;

import model.Measurements;

public class MeasurementTimeExtractor implements TimestampExtractor{
    
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partirionTime) {
        var measurementInfo = (Measurements) record.value();
        return measurementInfo.getProduceDate().toInstant().toEpochMilli(); // must use Optional if time can be null
    }
}
