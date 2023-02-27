package model;

import lombok.Data;
import java.util.Date;
import java.text.DecimalFormat;



@Data
public class TotalLeak {
    private double leak;
    private Date leakDate;
    private String leakType;

    public TotalLeak(double leak, Date leakDate, String leakType) {
        this.leak=leak;
        this.leakDate=leakDate;
        this.leakType=leakType;
    }

    public TotalLeak(double dayValue, double sumValue, Date tempLeakDate, String leakType){
        final DecimalFormat df1 = new DecimalFormat("0.00");

        leak = Double.parseDouble(df1.format(dayValue-sumValue));
        leakDate = tempLeakDate;
        this.leakType = leakType;
    }
}