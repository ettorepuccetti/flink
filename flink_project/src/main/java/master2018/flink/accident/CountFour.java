package master2018.flink.accident;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import master2018.flink.data.CarEvent;
import master2018.flink.data.Direction;

public class CountFour implements WindowFunction<CarEvent, AccidentEvent, Tuple, TimeWindow> {
    
    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<CarEvent> input, Collector<AccidentEvent> out) throws Exception {

        Iterator<CarEvent> iterator = input.iterator();
        //List<CarEvent> listevent = new ArrayList<CarEvent>();
        if (iterator.hasNext()) {
            CarEvent next = iterator.next();
            while(iterator.hasNext()) {
                out.collect(new AccidentEvent(next.getTime(), 0,
                        next.getVID(), next.getHighway(), next.getSegment(),
                        next.getDirection(), next.getPosition()));
                next = iterator.next();

            }
            out.collect(new AccidentEvent(next.getTime(), 0,
                        next.getVID(), next.getHighway(), next.getSegment(),
                        next.getDirection(), next.getPosition()));
            out.collect(new AccidentEvent(1111, 1111, 1111, 1111, 1111, next.getDirection(), 1111));
        }

        // iterator.forEachRemaining(listevent::add);
        
        // if (listevent.size() == 4) {
        //     CarEvent first = listevent.get(0);
        //     CarEvent last = listevent.get(3);
        //     out.collect(new AccidentEvent(first.getTime(), last.getTime(),
        //         first.getVID(), first.getHighway(), first.getSegment(),
        //         first.getDirection(), first.getPosition()));
        // }
    }
}