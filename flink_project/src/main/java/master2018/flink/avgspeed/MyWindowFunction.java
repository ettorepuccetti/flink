package master2018.flink.avgspeed;

import master2018.flink.data.CarEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

import static master2018.flink.avgspeed.AverageSpeed.END_SEGMENT;
import static master2018.flink.avgspeed.AverageSpeed.START_SEGMENT;

public class MyWindowFunction implements WindowFunction<CarEvent<Integer>, Event<Integer,Double>, Tuple, GlobalWindow> {

    // MS_TO_MPH is the scaling factor for converting meter/second to miles/hour
    private static double MS_TO_MPH = 2.23694;

    @Override
    public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<CarEvent<Integer>> iterable, Collector<Event<Integer,Double>> collector) {

        // store the event containing the westmost and eastmost position
        int westmostPosition   = Integer.MAX_VALUE;
        int eastmostPosition   = Integer.MIN_VALUE;
        CarEvent<Integer> westmostEvent = null;
        CarEvent<Integer> eastmostEvent = null;

        // find the westmost and eastmost position
        Iterator<CarEvent<Integer>> iterator = iterable.iterator();
        while(iterator.hasNext()) {
            CarEvent<Integer> n = iterator.next();
            // exclude segments out of scope (these were only used to trigger the window function)
            if(n.getSegment() < START_SEGMENT || n.getSegment() > END_SEGMENT) {
                continue;
            }
            // find the event with the westmost position
            if(n.getPosition() < westmostPosition) {
                westmostEvent = n;
                westmostPosition = n.getPosition();
            }
            // find the event with the eastmost position
            if(n.getPosition() > eastmostPosition) {
                eastmostEvent = n;
                eastmostPosition = n.getPosition();
            }
        }

        // avoid NullPointerExceptions (although they shouldn't happen)
        if(westmostEvent == null || eastmostEvent == null) {
            return;
        }
        // if a car entered the highway in between the start and end segment
        if(westmostEvent.getSegment() != START_SEGMENT || eastmostEvent.getSegment() != END_SEGMENT) {
            return;
        }

        // calculate average speed
        int positionDiff = eastmostEvent.getPosition() - westmostEvent.getPosition();
        int timeDiff = Math.abs(eastmostEvent.getTime() - westmostEvent.getTime());
        double ms = (double) positionDiff / (double) timeDiff;

        // convert to miles per hour
        double mph = ms * MS_TO_MPH;

        // add average speed event to the collector
        collector.collect(new Event<>(
                Math.min(westmostEvent.getTime(), eastmostEvent.getTime()),
                Math.max(westmostEvent.getTime(), eastmostEvent.getTime()),
                westmostEvent.getVID(),
                westmostEvent.getHighway(),
                westmostEvent.getDirection(),
                mph
        ));
    }
}

