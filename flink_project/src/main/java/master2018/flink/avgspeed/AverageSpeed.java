package master2018.flink.avgspeed;

import master2018.flink.Map;
import master2018.flink.data.CarEvent;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

public class AverageSpeed {

    // START_SEGMENT is the westmost segment on this route
    protected static int START_SEGMENT = 52;

    // END_SEGMENT is the eastmost segment on this route
    protected static int END_SEGMENT = 56;

    // KEY_BY is the field that CarEvents should be keyed by
    protected static String KEY_BY = "VID";

    public static void main(String[] args) {

        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<CarEvent> mapOutput = env
                .readTextFile(inFilePath).setParallelism(1)
                .map(new Map()).setParallelism(1);
        handleStream(mapOutput, outFilePath);

        try {
            env.execute("AverageSpeed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void handleStream(SingleOutputStreamOperator<CarEvent> mapOutput, String outfile) {
        mapOutput
                .filter(new SegmentFilter()).setParallelism(1)
                .keyBy(KEY_BY, "highway", "direction")
                .window(GlobalWindows.create())
                .trigger(new MyTrigger<>())
                .apply(new MyWindowFunction())
                .filter(new SpeedLimitFilter())
                .map(Event::asTuple)
                .writeAsCsv(outfile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
}
