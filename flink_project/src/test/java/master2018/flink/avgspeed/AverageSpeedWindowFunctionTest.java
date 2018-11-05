package master2018.flink.avgspeed;

import master2018.flink.data.CarEvent;
import master2018.flink.data.Direction;
import master2018.flink.data.Lane;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit test for MyWindowFunction, the window function used for the average speed calculation.
 */
public class AverageSpeedWindowFunctionTest {

    // Test the MyWindowFunction class
    @Test
    public void applyTest() throws Exception {
        // load a dummy csv file
        ArrayList<CarEvent> events = loadCsv("master2018/flink/avgspeed/avg-speed.csv");

        // test the function
        List<Event> output = new ArrayList<>();
        ListCollector<Event> collector = new ListCollector<>(output);
        MyWindowFunction averageSpeedWindowFunction = new MyWindowFunction();
        averageSpeedWindowFunction.apply(mock(Tuple.class), mock(GlobalWindow.class), events, collector);

        // we expect 1 speed limit with a average speed of 62
        assertEquals(1, output.size());
        assertEquals(62, Math.round(output.get(0).getAverageSpeed()));
    }

    // loadCsv loads a csv file with car events into an ArrayList of CarEvents
    private ArrayList<CarEvent> loadCsv(String filepath) throws FileNotFoundException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(filepath).getFile());
        Scanner scanner = new Scanner(file);

        ArrayList<CarEvent> out = new ArrayList<>();
        while(scanner.hasNext()) {
            String[] fieldArray = scanner.nextLine().split(",");
            out.add(new CarEvent(
                    Integer.parseInt(fieldArray[0]),
                    Integer.parseInt(fieldArray[1]),
                    Integer.parseInt(fieldArray[2]),
                    Integer.parseInt(fieldArray[3]),
                    Lane.values()[Integer.parseInt(fieldArray[4])],
                    Direction.values()[Integer.parseInt(fieldArray[5])],
                    Integer.parseInt(fieldArray[6]),
                    Integer.parseInt(fieldArray[7])
            ));
        }
        return out;
    }

}
