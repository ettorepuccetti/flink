package master2018.flink.speedlimit;

import master2018.flink.Map;
import master2018.flink.data.CarEvent;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SpeedLimit90 {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    

        SingleOutputStreamOperator<CarEvent> mapOutput = env
                .readTextFile(inFilePath).setParallelism(1)
                .map(new Map()).setParallelism(1);
        handleStream(mapOutput, outFilePath);

        try {
            env.execute("SpeedLimit90");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void handleStream(SingleOutputStreamOperator<CarEvent> mapOutput, String outfile) {
        mapOutput
                .filter(new Filter())
                .map(new OutputMap())
                .writeAsCsv(outfile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
}