package master2018.flink;

import master2018.flink.accident.AccidentDetection;
import master2018.flink.avgspeed.AverageSpeed;
import master2018.flink.data.CarEvent;
import master2018.flink.speedlimit.SpeedLimit90;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {

    /**
     * FILE_SPEED_FINES specifies the output file for speed fines
     */
    public static String FILE_SPEED_FINES = "speedfines.csv";

    /**
     * FILE_AVG_SPEED_FINES specifies the output file for average speed fines
     */
    public static String FILE_AVG_SPEED_FINES = "avgspeedfines.csv";

    /**
     * FILE_ACCIDENT_DETECTION specifies the output file for accident detection
     */
    public static String FILE_ACCIDENT_DETECTION = "accidents.csv";

    public static void main(String[] args) {

        String inFilePath = args[0];
        String outFilePath = args[1];

        // set up an execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // map all incoming events to CarEvents
        SingleOutputStreamOperator<CarEvent> mapOutput = env
                .readTextFile(inFilePath).setParallelism(1)
                .map(new Map()).setParallelism(1);

        // let each individual package handle its processing
        AverageSpeed.handleStream(mapOutput, outFilePath + "/" + FILE_AVG_SPEED_FINES);
        SpeedLimit90.handleStream(mapOutput, outFilePath + "/" + FILE_SPEED_FINES);
        AccidentDetection.handleStream(mapOutput, outFilePath + "/" + FILE_ACCIDENT_DETECTION);

        // execute the environment
        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
