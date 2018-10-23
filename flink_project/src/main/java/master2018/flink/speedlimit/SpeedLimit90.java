package master2018.flink.speedlimit;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

public class SpeedLimit90 {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> filterOut = 
            source.map(new Map())
                .filter(new Filter());

        filterOut.writeAsCsv(outFilePath);

        try {
            env.execute("ValerioSlideProgram1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}