package master2018.flink.accident;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.core.fs.FileSystem;

import master2018.flink.Map;

public class AccidentDetection {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
            .readTextFile(inFilePath).setMaxParallelism(1)
            .map(new Map()).setParallelism(1)
            .assignTimestampsAndWatermarks(new MyTimeAssigner())
            .filter(new FilterZero())
            .keyBy("VID","position")
            .window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30)))
            .apply(new CountFour())
            .map(AccidentEvent::asTuple)
            .writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("firstWindow");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


