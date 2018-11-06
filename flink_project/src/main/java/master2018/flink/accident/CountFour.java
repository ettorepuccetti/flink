package master2018.flink.accident;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import master2018.flink.data.CarEvent;
import master2018.flink.data.Direction;

public class CountFour implements WindowFunction<CarEvent, AccidentEvent, Tuple, GlobalWindow> {
    
    @Override
    public void apply(Tuple key, GlobalWindow window, Iterable<CarEvent> input, Collector<AccidentEvent> out) throws Exception {

        Iterator<CarEvent> iterator = input.iterator();
        List<CarEvent> listevent = new ArrayList<CarEvent>();

        iterator.forEachRemaining(listevent::add);
        
        if (listevent.size() == 4) {
            CarEvent first = listevent.get(0);
            CarEvent last = listevent.get(3);
            boolean timecorrect = true;
            int timestart = first.getTime();
            int i = 1;
            while (timecorrect && i < 4) {
                if (listevent.get(i).getTime() != timestart + 30*i) timecorrect = false;
                i++;
            }
            if (timecorrect) {
                out.collect(new AccidentEvent(
                    first.getTime(),
                    last.getTime(),
                    first.getVID(), 
                    first.getHighway(),
                    first.getSegment(),
                    first.getDirection(),
                    first.getPosition()));
            }
        }
    }
}