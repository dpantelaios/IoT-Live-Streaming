package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TotalMovesMeasurement {
    
    private Date totalMovesDate;
    private int totalMoves;

}