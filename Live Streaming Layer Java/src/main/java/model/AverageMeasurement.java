package model;
import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;
import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AverageMeasurement {

    // @JsonFormat(shape = JsonFormat.Shape.STRING,
    //         pattern = "yyyy-MM-dd hh:mm:ss")
    // private Date measurement_time;

    private float addedValues;
    private int count;
    private float avgMeasurement;

    // public AverageMeasurement(final float addV, final int cnt, final float avg) {
    //     this.addedValues = addV;
    //     this.count = cnt;
    //     this.avgMeasurement = avg;
    // }
    
}