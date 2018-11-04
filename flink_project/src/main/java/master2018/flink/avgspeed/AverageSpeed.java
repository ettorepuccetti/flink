package master2018.flink.avgspeed;

import master2018.flink.Map;
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

        env
                .readTextFile(inFilePath)
                .map(new Map())
                .filter(new SegmentFilter())
                .keyBy(KEY_BY)
                .window(GlobalWindows.create())
                .trigger(new MyTrigger<>())
                .apply(new MyWindowFunction())
                .filter(new SpeedLimitFilter())
                .map(Event::asTuple)
                .writeAsCsv(outFilePath);

        try {
            env.execute("avgspeed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
