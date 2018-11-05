package master2018.flink.avgspeed;

import master2018.flink.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * AverageSpeedTest provides integration tests for the average speed exercise.
 */
public class AverageSpeedTest {

    // Test to see what happens when a car exceeds the maximum speed limit.
    @Test
    public void testAverageSpeed() throws Exception {
        List<Event> events = runTest("master2018/flink/avgspeed/avg-speed.csv");

        // verify your results
        assertEquals(1, events.size());
        assertEquals(62, Math.round(events.get(0).getAverageSpeed()));
    }

    // Test to see what happens when a car leaves and enters in between the segments
    @Test
    public void testExitAndEnter() throws Exception {
        List<Event> events = runTest("master2018/flink/avgspeed/exit-and-enter.csv");

        // verify your results
        assertEquals(0, events.size());
    }

    // Test to see what happens when a car leaves in between the segments
    @Test
    public void testExitInBetween() throws Exception {
        List<Event> events = runTest("master2018/flink/avgspeed/exit-in-between.csv");

        // verify your results
        assertEquals(0, events.size());
    }

    // Test to see what happens when a car enters in between the segments
    @Test
    public void testEnterInBetween() throws Exception {
        List<Event> events = runTest("master2018/flink/avgspeed/enter-in-between.csv");

        // verify your results
        assertEquals(0, events.size());
    }

    // Test to see what happens when a car goes back and forth on the same track
    @Test
    public void testBackAndForth() throws Exception {
        List<Event> events = runTest("master2018/flink/avgspeed/back-and-forth.csv");

        // verify your results
        assertEquals(2, events.size());
        assertEquals(62, Math.round(events.get(0).getAverageSpeed()));
        assertEquals(67, Math.round(events.get(1).getAverageSpeed()));
    }

    // Test to see what happens when a car does not speed
    @Test
    public void testGoodSpeed() throws Exception {
        List<Event> events = runTest("master2018/flink/avgspeed/good-speed.csv");

        // verify your results
        assertEquals(0, events.size());
    }

    // Test to see if we can handle two distinct cars speeding
    @Test
    public void testTwoCars() throws Exception {
        List<Event> events = runTest("master2018/flink/avgspeed/two-cars.csv");

        // verify your results
        assertEquals(2, events.size());
        assertEquals(1, Math.round(events.get(0).getVID()));
        assertEquals(62, Math.round(events.get(0).getAverageSpeed()));
        assertEquals(2, Math.round(events.get(1).getVID()));
        assertEquals(67, Math.round(events.get(1).getAverageSpeed()));
    }

    // Test to see if we can handle only one of two cars speeding
    @Test
    public void testOneOfTwoCars() throws Exception {
        List<Event> events = runTest("master2018/flink/avgspeed/one-of-two-cars.csv");

        // verify your results
        assertEquals(1, events.size());
        assertEquals(1, Math.round(events.get(0).getVID()));
        assertEquals(62, Math.round(events.get(0).getAverageSpeed()));
    }

    // Run an end-to-end test. This test is supposed to resemble the full AverageSpeed pipeline.
    private List<Event> runTest(String filepath) throws Exception {
        // Get the resource filepath
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(filepath).getFile());

        // Get the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // create a new collect sink
        CollectSink.events.clear();

        // Simulate the AverageSpeed pipeline
        env
                .readTextFile(file.getAbsolutePath())
                .map(new Map())
                .filter(new SegmentFilter())
                .keyBy("VID")
                .window(GlobalWindows.create())
                .trigger(new MyTrigger<>())
                .apply(new MyWindowFunction())
                .filter(new SpeedLimitFilter())
                .addSink(new CollectSink());

        // execute
        env.execute();

        return CollectSink.events;
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Event> {

        // must be static
        public static final List<Event> events = new ArrayList<>();

        @Override
        public synchronized void invoke(Event event) {
            events.add(event);
        }
    }

}
