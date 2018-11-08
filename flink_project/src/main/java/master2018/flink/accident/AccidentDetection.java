package master2018.flink.accident;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import master2018.flink.Map;
import master2018.flink.data.CarEvent;

public class AccidentDetection {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<CarEvent> mapOutput = env
            .readTextFile(inFilePath).setParallelism(1)
            .map(new Map()).setParallelism(1);

        handleStream(mapOutput, outFilePath);  

        try {
            env.execute("AccidentDetection");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void handleStream(SingleOutputStreamOperator<CarEvent> mapOutput, String outfile) {
        mapOutput
            .filter(new FilterZero()).setParallelism(1)
            .keyBy("VID","position","direction")
            .countWindow(4, 1)
            .apply(new CheckConsecutive())
            .map(AccidentEvent::asTuple)
            .writeAsCsv(outfile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
}


