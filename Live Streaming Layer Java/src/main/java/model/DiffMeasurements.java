package model;
import com.fasterxml.jackson.annotation.JsonFormat;

import org.apache.kafka.common.serialization.Serdes;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;
import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DiffMeasurements {
    
    private float previousValue;
    private float currentValue;
    private Date diffDate; 

}