package model;
import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;
import java.time.LocalDate;
import java.text.DecimalFormat;



@Data
public class TotalLeak {
    private double leak;
    // private double joinEnergyDiff;
    private Date leakDate;


    public TotalLeak(double dayValue, double sumValue, Date tempLeakDate){
        final DecimalFormat df1 = new DecimalFormat("0.00");

        leak = Double.parseDouble(df1.format(dayValue-sumValue));
        leakDate = tempLeakDate;
    }
}