package boris_ettore;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import scala.Int;

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
            source.map(new MyMap())
                .filter(new MyFilter());

        filterOut.writeAsCsv(outFilePath);

        try {
            env.execute("ValerioSlideProgram1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static final class MyMap 
        implements MapFunction<String,Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {
        
        @Override
        public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map (String in) throws Exception {
            String[] fieldArray = in.split(",");
            Tuple6<Integer,Integer,Integer,Integer,Integer, Integer> out = 
                new Tuple6(Integer.parseInt(fieldArray[0]),Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[3]),
                    Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[2]));
            return out;
        }
    }

    public static final class MyFilter 
        implements FilterFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {
       
        @Override
        public boolean filter (Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> in) throws Exception {
            return (in.f5 > 90);
        }
    }
}