package master2018.flink.speedlimit;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class SpeedLimit90 {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        env.
            readTextFile(inFilePath)
            .map(new Map())
            .filter(new Filter())
            .writeAsCsv(outFilePath).setParallelism(1);

        try {
            env.execute("speedlimit90");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}