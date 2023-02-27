package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedMeasurement {

    private double value;
    private Date produceDate;
    private String sensorName;

}