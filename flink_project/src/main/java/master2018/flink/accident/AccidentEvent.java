package master2018.flink.accident;

import org.apache.flink.api.java.tuple.Tuple7;

import master2018.flink.data.Direction;

public class AccidentEvent {

    int time1;
    int time2;
    int VID;
    int speed;
    int highway;
    Direction direction;
    int segment;
    int position;

    public AccidentEvent(int time1, int time2, int VID, int highway, int segment, Direction direction, int position) {

        this.time1 = time1;
        this.time2 = time2;
        this.VID = VID;
        this.highway = highway;
        this.direction = direction;
        this.segment = segment;
        this.position = position;
    }

    public Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> asTuple() {
        return new Tuple7<>(
                time1,
                time2,
                VID,
                highway,
                direction.ordinal(),
                segment,
                position
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

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public int getHighway() {
        return highway;
    }

    public void setHighway(int highway) {
        this.highway = highway;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public int getSegment() {
        return segment;
    }

    public void setSegment(int segment) {
        this.segment = segment;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }
}
