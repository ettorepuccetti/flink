package master2018.flink.accident;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.core.fs.FileSystem;


import java.util.Iterator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class windows_es {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> mapStream = source.map(new MyMap());

        KeyedStream<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>,Tuple> keyedStream = mapStream.assignTimestampsAndWatermarks(new MyAscending()).keyBy(1,7);
        
        SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> filterEvents = keyedStream
            .window(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(1)))
            .apply(new CountFour());

        filterEvents.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);


        try {
            env.execute("firstWindow");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final class MyMap
        implements MapFunction<String, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> {

            @Override
            public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> map (String in) throws Exception {
            String[] fieldArray = in.split(",");
            Tuple8<Integer,Integer,Integer,Integer,Integer, Integer, Integer, Integer> out =
                    new Tuple8(Integer.parseInt(fieldArray[0]),Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[2]),
                            Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[4]),Integer.parseInt(fieldArray[5]),
                            Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[7]));
            return out;
        }
    }

//KeyedStream<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple> keyedStream = mapStream.assignTimestampsAndWatermarks(new

    public static final class MyAscending extends AscendingTimestampExtractor<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> {

        @Override
        public long extractAscendingTimestamp(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> element) {
            return element.f0*1000;
        }
    }





    public static final class CountFour implements 
        WindowFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>,
        Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple, TimeWindow> {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void apply(Tuple key, TimeWindow window,
             Iterable<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> input,
             Collector<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> out) 
             throws Exception {

                Iterator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> iterator = input.iterator();
                Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> first = iterator.next();
                Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> next = first;
                int counter = 0;
                
                if (iterator.hasNext()) {
                    out.collect(new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(1,1,1,1,1,1,1,1));
                    out.collect(new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(first.f0, first.f1, first.f2, first.f3, first.f4, first.f5, first.f6, first.f7));
                    while (iterator.hasNext()) {
                        next = iterator.next();
                        out.collect(new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(next.f0, next.f1, next.f2, next.f3, next.f4, next.f5, next.f6, next.f7));
                    }
                }
                

                // if(first != null) {
                //     counter++;
                // }
                // while(iterator.hasNext()) {
                //     next = iterator.next();
                //     counter++;
                // }
                // if (counter == 4)
                // out.collect(new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(first.f0, next.f0, first.f1, next.f1, first.f2, next.f2, first.f3, next.f3));
                
            }
    }
}


