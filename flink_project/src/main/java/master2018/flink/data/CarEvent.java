package master2018.flink.data;


import org.apache.flink.api.java.tuple.Tuple8;

public class CarEvent extends Tuple8<Integer,Integer,Integer,Integer,Lane,Direction,Integer,Integer> {

//    public CarEvent(int time, int VID, int speed, int highway, Lane lane, Direction direction, int segment, int position) {
//        this.f0 = time;
//        this.f1 = VID;
//        this.f2 = speed;
//        this.f3 = highway;
//        this.f4 = lane;
//        this.f5 = direction;
//        this.f6 = segment;
//        this.f7 = position;
//    }

    public int getTime() {
        return f0;
    }

    public void setTime(int time) {
        this.f0 = time;
    }

    public int getVID() {
        return f1;
    }

    public void setVID(int VID) {
        this.f1 = VID;
    }

    public int getSpeed() {
        return f2;
    }

    public void setSpeed(int speed) {
        this.f2 = speed;
    }

    public int getHighway() {
        return f3;
    }

    public void setHighway(int highway) {
        this.f3 = highway;
    }

    public Lane getLane() {
        return f4;
    }

    public void setLane(Lane lane) {
        this.f4 = lane;
    }

    public Direction getDirection() {
        return f5;
    }

    public void setDirection(Direction direction) {
        this.f5 = direction;
    }

    public int getSegment() {
        return f6;
    }

    public void setSegment(int segment) {
        this.f6 = segment;
    }

    public int getPosition() {
        return f7;
    }

    public void setPosition(int position) {
        this.f7 = position;
    }
}