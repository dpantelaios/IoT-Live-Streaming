package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DiffMeasurement {
    
    private double previousValue;
    private double currentValue;
    private Date diffDate;
    private String sensorName;

}