import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;
import java.time.LocalDate;


public class AggregatedMeasurement {

    @JsonFormat(shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd hh:mm:ss")
    private Date measurement_time;
    private static Integer total_measurement = 0;

    public AggregatedMeasurement(Date measurement_time){
        this.measurement_time = measurement_time;
    }

    private Date getdate(){
        return measurement_time;
    }

    private Integer getTotalMeasurement(){
        return total_measurement;
    }

    private void AddMeasurement(Integer sensor_measurement){
        total_measurement += sensor_measurement;
    }
}