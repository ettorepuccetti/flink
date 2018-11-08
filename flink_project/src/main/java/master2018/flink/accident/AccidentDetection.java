package master2018.flink.accident;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import master2018.flink.Map;

public class AccidentDetection {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
            .readTextFile(inFilePath).setParallelism(1)
            .map(new Map()).setParallelism(1)
            .filter(new FilterZero()).setParallelism(1)
            .keyBy("VID","position","direction")
            .countWindow(4, 1)
            .apply(new CheckConsecutive())
            .map(AccidentEvent::asTuple)
            .writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("firstWindow");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


