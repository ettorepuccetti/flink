package boris_ettore.class_exercises;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

public class quarto_es {
    public static void main(String[] args) {
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        DataStreamSource<String> source = env.readTextFile(inFilePath);
        
        SingleOutputStreamOperator<Tuple3<Long,String,Double>> mappedOut = source.map(new MyMap());
        
        KeyedStream<Tuple3<Long,String,Double>, Tuple> keyedStream = mappedOut.keyBy(1);

        SingleOutputStreamOperator<Tuple3<Long,String,Double>> reduce = keyedStream.reduce(new myReduce());

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> min = keyedStream.min(2);

        min.print();
        
        reduce.writeAsCsv(outFilePath);

        try {
            env.execute("ValerioSlideProgram1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final class myReduce implements ReduceFunction<Tuple3<Long,String,Double>> {
        // hint: value1 e` il valore della tupla ritornata precedentemente, value2 la tupla corrente
        @Override
        public Tuple3<Long, String, Double> reduce (Tuple3<Long, String, Double> value1, Tuple3<Long, String, Double> value2) throws Exception {
            Tuple3<Long, String, Double> out = new Tuple3<Long, String, Double>(value2.f0, value1.f1, value1.f2+value2.f2);
            return out;
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
}

//env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);