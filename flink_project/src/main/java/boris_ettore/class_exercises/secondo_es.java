package boris_ettore.class_exercises;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

public class secondo_es {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple3<Long,String,Double>> filterOut = 
            source.map(new MyMap())
                .filter(new MyFilter());

        filterOut.writeAsCsv(outFilePath);

        try {
            env.execute("ValerioSlideProgram1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static final class MyMap implements MapFunction<String,Tuple3<Long,String,Double>> {
        
        @Override
        public Tuple3<Long,String,Double> map (String in) throws Exception {
            String[] fieldArray = in.split(",");
            Tuple3<Long, String, Double> out = new Tuple3(Long.parseLong(fieldArray[0]), fieldArray[1],
                Double.parseDouble(fieldArray[2]));
            return out;
        }
    }

    public static final class MyFilter implements FilterFunction<Tuple3<Long,String,Double>> {
       
        @Override
        public boolean filter(Tuple3<Long, String, Double> in) throws Exception {
            if(in.f1.equals("sensor1")){ return true;
            }else{ return false; }
        }

    }
}