package model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MaxMeasurement {
    private double maxValue;
    private Date maxDate;
    private String sensorName;
}
