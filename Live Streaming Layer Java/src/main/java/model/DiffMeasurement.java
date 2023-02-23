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
public class DiffMeasurement {
    
    private double previousValue;
    private double currentValue;
    private Date diffDate;
    private String sensorName;

}