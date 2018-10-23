package master2018.flink.speedlimit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public final class Map
        implements MapFunction<String, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {

    @Override
    public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map (String in) throws Exception {
        String[] fieldArray = in.split(",");
        Tuple6<Integer,Integer,Integer,Integer,Integer, Integer> out =
                new Tuple6(Integer.parseInt(fieldArray[0]),Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[3]),
                        Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[2]));
        return out;
    }
}

