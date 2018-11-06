package master2018.flink.avgspeed;

import master2018.flink.data.Direction;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Event is an average speed event as expected for the output CSV file.
 */
public class Event extends Tuple6<Integer, Integer, Integer, Integer, Direction, Double> {

    public Event(int time1, int time2, int VID, int highway, Direction direction, double averageSpeed) {
        this.f0 = time1;
        this.f1 = time2;
        this.f2 = VID;
        this.f3 = highway;
        this.f4 = direction;
        this.f5 = averageSpeed;
    }

    public int getTime1() {
        return f0;
    }

    public void setTime1(int time1) {
        this.f0 = time1;
    }

    public int getTime2() {
        return f1;
    }

    public void setTime2(int time2) {
        this.f1 = time2;
    }

    public int getVID() {
        return f2;
    }

    public void setVID(int VID) {
        this.f2 = VID;
    }

    public int getHighway() {
        return f3;
    }

    public void setHighway(int highway) {
        this.f3 = highway;
    }

    public Direction getDirection() {
        return f4;
    }

    public void setDirection(Direction direction) {
        this.f4 = direction;
    }

    public double getAverageSpeed() {
        return f5;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.f5 = averageSpeed;
    }
}
