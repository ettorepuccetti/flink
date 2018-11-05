package master2018.flink.avgspeed;

import master2018.flink.data.CarEvent;
import org.apache.flink.api.common.functions.FilterFunction;

import static master2018.flink.avgspeed.AverageSpeed.END_SEGMENT;
import static master2018.flink.avgspeed.AverageSpeed.START_SEGMENT;

/**
 * SegmentFilter filters out all segments that are not needed for the average speed calculation.
 * Note that we include the segments before and after the start and end segment respectively.
 */
public class SegmentFilter implements FilterFunction<CarEvent> {
    @Override
    public boolean filter(CarEvent carEvent) {
        if(carEvent.getSegment() >= START_SEGMENT - 1 && carEvent.getSegment() <= END_SEGMENT + 1) {
            return true;
        }
        return false;
    }
}
