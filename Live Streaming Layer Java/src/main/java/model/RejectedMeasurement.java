package model;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import model.CustomDateDeserializer;
import org.apache.kafka.common.serialization.Serdes;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;
import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RejectedMeasurement {

    private double value;
    private Date produceDate;
    public Date withouttimeDate(){
        Date test = produceDate;
        test.setHours(0);
        test.setMinutes(0);
        test.setSeconds(0);
        return test;
    }

    public long timeDatehelp(){
        Date test = produceDate;
        return test.toInstant().toEpochMilli();
    }
}