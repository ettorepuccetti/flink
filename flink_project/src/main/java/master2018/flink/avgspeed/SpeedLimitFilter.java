package master2018.flink.avgspeed;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filter out all average speeds exceeding the maximum allowed speed.
 */
public class SpeedLimitFilter implements FilterFunction<Event> {

    private static double MAX_SPEED = 60;

    @Override
    public boolean filter(Event event) {
        return (event.getAverageSpeed() > MAX_SPEED);
    }
}
