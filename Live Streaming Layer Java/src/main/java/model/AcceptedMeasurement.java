package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AcceptedMeasurement {

    private double value;
    private Date produceDate;

    public Date withouttimeDate(){
        Date test = produceDate;
        test.setHours(0);
        test.setMinutes(0);
        test.setSeconds(0);
        return test;
    }

}