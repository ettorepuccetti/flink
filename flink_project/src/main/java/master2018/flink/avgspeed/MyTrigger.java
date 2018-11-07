package master2018.flink.avgspeed;

import master2018.flink.data.CarEvent;
import master2018.flink.data.Direction;
import master2018.flink.data.Lane;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import static master2018.flink.avgspeed.AverageSpeed.END_SEGMENT;
import static master2018.flink.avgspeed.AverageSpeed.START_SEGMENT;

/**
 * MyTrigger implements the trigger function for the AverageSpeed pipeline.
 * It triggers on the following combinations:
 * - A car travels westwards and hits segment 51: fire the window function
 * - A car travels eastwards and hits segment 57: fire the window function
 * - A car exits: purge all window information
 * @param <T> CarEvent
 * @param <W> Window
 */
public class MyTrigger<T extends CarEvent<Integer>, W extends Window> extends Trigger<T, W> {

    /**
     * onElement implements the triggering logic.
     * @param t CarEvent
     * @param l ?
     * @param w Window
     * @param triggerContext context
     * @return The action
     */
    @Override
    public TriggerResult onElement(T t, long l, W w, TriggerContext triggerContext) {
        if (t.getSegment() == START_SEGMENT - 1 && t.getDirection() == 1) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        if (t.getSegment() == END_SEGMENT + 1 && t.getDirection() == 0) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        if (t.getLane() == 4) {
            return TriggerResult.PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, W w, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, W w, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W w, TriggerContext triggerContext) throws Exception {

    }
}

