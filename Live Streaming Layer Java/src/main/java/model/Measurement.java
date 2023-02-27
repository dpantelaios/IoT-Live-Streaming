package model;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Measurement {

    private double value;
    @JsonDeserialize(using= CustomDateDeserializer.class)
    private Date produceDate;

}