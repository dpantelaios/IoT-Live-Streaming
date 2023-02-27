package model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SummedMeasurement {
    private double sum;
    private Date aggregationDate;
    private String sensorName;
}
