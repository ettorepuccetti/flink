package master2018.flink.avgspeed;

import master2018.flink.Map;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

public class AverageSpeed {

    // START_SEGMENT is the westmost segment on this route
    protected static int START_SEGMENT = 52;

    // END_SEGMENT is the eastmost segment on this route
    protected static int END_SEGMENT = 56;

    // KEY_BY is the field that CarEvents should be keyed by
    protected static int VID_KEY = 1;

    public static void main(String[] args) {

        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .readTextFile(inFilePath).setParallelism(1)
                .map(new Map()).setParallelism(1)
                .filter(new SegmentFilter()).setParallelism(1)
                // TODO: 07/11/2018 check if keyBy is sufficient now
//                .keyBy(VID_KEY, "highway", "direction")
                .keyBy(VID_KEY)
                .window(GlobalWindows.create())
                .trigger(new MyTrigger<>())
                .apply(new MyWindowFunction())
                .filter(new SpeedLimitFilter())
                .writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("avgspeed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
