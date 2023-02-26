import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import lombok.SneakyThrows;

import model.Measurement;
import model.TotalMovesMeasurement;

public class totalMovesTransformer implements Transformer<String, Measurement, KeyValue<String, TotalMovesMeasurement>>{

    private ProcessorContext context;
    private int totalMoves;
    private String stateStoreName;
    

    public totalMovesTransformer() {
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
        // System.out.println(lastSeen);
        return KeyValue.pair(key, newVal);
    }

    @Override
    public void close() {}

}
