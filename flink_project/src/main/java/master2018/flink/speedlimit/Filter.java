package master2018.flink.speedlimit;

import master2018.flink.data.CarEvent;
import org.apache.flink.api.common.functions.FilterFunction;

public final class Filter implements FilterFunction<CarEvent> {

    @Override
    public boolean filter (CarEvent carEvent) {
        return (carEvent.getSpeed() > 90);
    }
}

