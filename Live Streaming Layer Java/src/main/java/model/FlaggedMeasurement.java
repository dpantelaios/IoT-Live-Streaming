package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlaggedMeasurement {

    private double value;
    private String isRejected;
    private Date produceDate;

}