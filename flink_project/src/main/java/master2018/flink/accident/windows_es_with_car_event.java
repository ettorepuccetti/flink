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
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class windows_es_with_car_event {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<CarEvent> mapStream = source.map(new MyMap());

        KeyedStream<CarEvent,Tuple> keyedStream = mapStream.assignTimestampsAndWatermarks(new MyAscending()).keyBy(1,5,7);
        
        SingleOutputStreamOperator<AccidentEvent> filterEvents = keyedStream
            .window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30)))
            .apply(new CountFour());
        
        SingleOutputStreamOperator<CarEvent> debugPurpose = keyedStream
            .window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30)))
            .apply(new debugPrint());

        filterEvents.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);
        debugPurpose.writeAsCsv("~/maven-projects/flink/data/debugAccident.csv", FileSystem.WriteMode.OVERWRITE);


        try {
            env.execute("firstWindow");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final class MyMap implements MapFunction<String, CarEvent> {

            @Override
            public CarEvent map (String in) throws Exception {
            String[] fieldArray = in.split(",");
            CarEvent out = new CarEvent(Integer.parseInt(fieldArray[0]),Integer.parseInt(fieldArray[1]),
                Integer.parseInt(fieldArray[2]),Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[4]),
                Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[7]));
            return out;
        }
    }


    public static final class MyAscending extends AscendingTimestampExtractor<CarEvent> {

        @Override
        public long extractAscendingTimestamp(CarEvent element) {
            return element.getTime()*1000;
        }
    }



    public static final class CountFour implements WindowFunction<CarEvent,AccidentEvent, Tuple, TimeWindow> {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void apply(Tuple key, TimeWindow window, Iterable<CarEvent> input, Collector<AccidentEvent> out) throws Exception {

                Iterator<CarEvent> iterator = input.iterator();
                List<CarEvent> listevent = new ArrayList<CarEvent>();
                iterator.forEachRemaining(listevent::add);
                if (listevent.size() > 4) {
                    throw new Exception("!! captured more than 4 events in a windows of size 4 !! ");
                }

                if (listevent.size() < 4) {
                    return;
                }
                
                CarEvent first = listevent.get(0);
                CarEvent last = listevent.get(3);
				out.collect(new AccidentEvent(first.getTime(), last.getTime(),
                    first.getVID(), first.getHighway(), first.getSegment(),
                    first.getDirection(), first.getPosition()));
            }
    }

    public static final class debugPrint implements WindowFunction<CarEvent,CarEvent, Tuple, TimeWindow> {
            
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<CarEvent> input, Collector<CarEvent> out) throws Exception {

            Iterator<CarEvent> iterator = input.iterator();
            while (iterator.hasNext()) {
                CarEvent next = iterator.next();
                out.collect(next);
            }
        }
}
}


