package master2018.flink.accident;

import master2018.flink.data.CarEvent;
import org.apache.flink.api.common.functions.FilterFunction;


public class FilterZero implements FilterFunction<CarEvent> {
    @Override
    public boolean filter(CarEvent carEvent) {
        if(carEvent.getSpeed() == 0) return true;
        else return false;
    }
}