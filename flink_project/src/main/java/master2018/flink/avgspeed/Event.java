package master2018.flink.avgspeed;

import master2018.flink.data.Direction;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Event is an average speed event as expected for the output CSV file.
 */
public class Event {

    int time1;
    int time2;
    int VID;
    int highway;
    int direction;
    double averageSpeed;

    public Event(int time1, int time2, int VID, int highway, int direction, double averageSpeed) {
        this.time1 = time1;
        this.time2 = time2;
        this.VID = VID;
        this.highway = highway;
        this.direction = direction;
        this.averageSpeed = averageSpeed;
    }

    public Tuple6<Integer, Integer, Integer, Integer, Integer, Double> asTuple() {
        return new Tuple6<>(
                time1,
                time2,
                VID,
                highway,
                direction,
                averageSpeed
        );
    }

    public int getTime1() {
        return time1;
    }

    public void setTime1(int time1) {
        this.time1 = time1;
    }

    public int getTime2() {
        return time2;
    }

    public void setTime2(int time2) {
        this.time2 = time2;
    }

    public int getVID() {
        return VID;
    }

    public void setVID(int VID) {
        this.VID = VID;
    }

    public int getHighway() {
        return highway;
    }

    public void setHighway(int highway) {
        this.highway = highway;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }
}
