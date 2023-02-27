package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LeakSum {
    private double leakSum;
    private int leakSumCount;
    private Date leakSumDate;
}