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
    // @JsonFormat(shape = JsonFormat.Shape.STRING,
    //         pattern = "yyyy-MM-dd hh:mm:ss")
    // private Date produce_Date;
    // private float value_2;


    // public Measurement(Date measurement_time, Integer measurement){
    //     this.measurement_time = measurement_time;
    //     this.measurement = measurement;
    // }

    // public Date getdate(){
    //     return produceDate;
    // }

    // public float getMeasurement(){
    //     return value;
    // }

    // private Date getDateFromDatetime(){
    //     LocalDate time1 = LocalDate.parse(measurement_time);
    //     date_return = new Date();
    //     date_return.setYear(time1.getYear());
    //     date_return.setMonth(time1.getMonth());
    //     date_return.setDay(time1.getDay());
    //     date_return.setHour(0);
    //     date_return.setMinute(0);
    //     date_return.setSecond(0);
    //     return date_return;
    // }

}