package master2018.flink.accident;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import master2018.flink.data.CarEvent;

public class CheckConsecutive implements WindowFunction<CarEvent, AccidentEvent, Tuple, GlobalWindow> {
    
    @Override
    public void apply(Tuple key, GlobalWindow window, Iterable<CarEvent> input, Collector<AccidentEvent> out) {

        Iterator<CarEvent> iterator = input.iterator();
        List<CarEvent> listevent = new ArrayList<CarEvent>();

        iterator.forEachRemaining(listevent::add);
        
        if (listevent.size() == 4) {
            CarEvent first = listevent.get(0);
            CarEvent last = listevent.get(3);
            boolean timeConsecutive = true;
            int timeStart = first.getTime();
            int i = 1;
            while (timeConsecutive && i < 4) {
                if (listevent.get(i).getTime() != timeStart + 30*i) timeConsecutive = false;
                i++;
            }
            if (timeConsecutive) {
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