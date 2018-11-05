package master2018.flink;

import master2018.flink.data.CarEvent;
import master2018.flink.data.Direction;
import master2018.flink.data.Lane;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public final class Map
        implements MapFunction<String, CarEvent> {

    @Override
    public CarEvent map (String in) {
        String[] fieldArray = in.split(",");
        return new CarEvent(
                Integer.parseInt(fieldArray[0]),
                Integer.parseInt(fieldArray[1]),
                Integer.parseInt(fieldArray[2]),
                Integer.parseInt(fieldArray[3]),
                Lane.values()[Integer.parseInt(fieldArray[4])],
                Direction.values()[Integer.parseInt(fieldArray[5])],
                Integer.parseInt(fieldArray[6]),
                Integer.parseInt(fieldArray[7])
        );
    }
}

