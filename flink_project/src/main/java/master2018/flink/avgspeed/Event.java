package master2018.flink.avgspeed;

import master2018.flink.data.Direction;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Event is an average speed event as expected for the output CSV file.
 */
public class Event<A,B> extends Tuple6<A,A,A,A,A,B> {

    public Event(A time1, A time2, A VID, A highway, A direction, B averageSpeed) {
        this.f0 = time1;
        this.f1 = time2;
        this.f2 = VID;
        this.f3 = highway;
        this.f4 = direction;
        this.f5 = averageSpeed;
    }

    public A getTime1() {
        return f0;
    }

    public void setTime1(A time1) {
        this.f0 = time1;
    }

    public A getTime2() {
        return f1;
    }

    public void setTime2(A time2) {
        this.f1 = time2;
    }

    public A getVID() {
        return f2;
    }

    public void setVID(A VID) {
        this.f2 = VID;
    }

    public A getHighway() {
        return f3;
    }

    public void setHighway(A highway) {
        this.f3 = highway;
    }

    public A getDirection() {
        return f4;
    }

    public void setDirection(A direction) {
        this.f4 = direction;
    }

    public B getAverageSpeed() {
        return f5;
    }

    public void setAverageSpeed(B averageSpeed) {
        this.f5 = averageSpeed;
    }
}
