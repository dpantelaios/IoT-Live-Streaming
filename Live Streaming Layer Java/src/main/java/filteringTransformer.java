import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import model.Measurement;
import model.FlaggedMeasurement;

public class FilteringTransformer implements Transformer<String, Measurement, KeyValue<String, FlaggedMeasurement>>{

    private ProcessorContext context;
    private long lastSeen;
    private String stateStoreName;
    

    public FilteringTransformer(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<String, FlaggedMeasurement> transform(String key, Measurement value) {
        long valueTime = value.getProduceDate().getTime();
        FlaggedMeasurement newVal;
        if(lastSeen-valueTime>=864000000) {
            newVal = new FlaggedMeasurement(value.getValue(), "true", value.getProduceDate());
        }
        else {
            newVal = new FlaggedMeasurement(value.getValue(), "false", value.getProduceDate());
            if(lastSeen<valueTime)
                lastSeen=valueTime;
        }
        return KeyValue.pair(key, newVal);
    }

    @Override
    public void close() {}

}
