package master2018.flink.speedlimit;

import master2018.flink.data.CarEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public final class OutputMap
        implements MapFunction<CarEvent, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {

    @Override
    public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map (CarEvent carEvent) {
        return new Tuple6<>(
                carEvent.getTime(),
                carEvent.getVID(),
                carEvent.getHighway(),
                carEvent.getSegment(),
                carEvent.getDirection().ordinal(),
                carEvent.getSpeed()
        );
    }
}

