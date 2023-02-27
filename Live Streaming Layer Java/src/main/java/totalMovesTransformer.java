import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import model.Measurement;
import model.TotalMovesMeasurement;

public class TotalMovesTransformer implements Transformer<String, Measurement, KeyValue<String, TotalMovesMeasurement>>{

    private ProcessorContext context;
    private int totalMoves;
    private String stateStoreName;
    

    public TotalMovesTransformer() {
        this.totalMoves = 0;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<String, TotalMovesMeasurement> transform(String key, Measurement value) {
        TotalMovesMeasurement newVal;
        totalMoves += 1;
        newVal = new TotalMovesMeasurement(value.getProduceDate(), totalMoves);
        return KeyValue.pair(key, newVal);
    }

    @Override
    public void close() {}

}
