package master2018.flink.speedlimit;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public final class Filter
        implements FilterFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {

    @Override
    public boolean filter (Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> in) throws Exception {
        return (in.f5 > 90);
    }
}

