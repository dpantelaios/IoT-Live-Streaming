package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FilterLateEvents {
    private String isLateEvent;
    private Date maxDate;
    private double filteredValue;
    private Date filteredProduceDate;

    public String lateEvent(Date measurementDate){
        long diffInMillies = maxDate.getTime() - measurementDate.getTime();
        if(diffInMillies < 864000000){ //true if the difference is less than 10 days
            return "false";
        }
        else{
            return "true";
        }
    }

    public void updateMaxDate(Date measurementDate){
        if(measurementDate.getTime()/1000 == 1589673600){ //1589673600000 ->17/5/2020 00:00:00
            maxDate = measurementDate; //initialize Date at the beginning, without window stays the same as in the last execution
        }
        if(measurementDate.getTime() > maxDate.getTime()){
            maxDate = measurementDate;
        }
        return;
    }
}
