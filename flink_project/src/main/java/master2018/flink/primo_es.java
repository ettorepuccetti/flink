package master2018.flink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class primo_es {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        String inFilePath = args[0];
        String outFilePath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath);
        source.writeAsText(outFilePath);
        
        try {
            env.execute("ValerioSlideProgram1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}