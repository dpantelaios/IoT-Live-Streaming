package model;
import com.fasterxml.jackson.annotation.JsonFormat;

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
public class Measurements {

    @JsonFormat(shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd HH:mm:ss")
    private Date produceDate;
    private float value;

    public Date withouttimeDate(){
        Date test = produceDate;
        test.setHours(0);
        test.setMinutes(0);
        test.setSeconds(0);
        return test;
    }
}