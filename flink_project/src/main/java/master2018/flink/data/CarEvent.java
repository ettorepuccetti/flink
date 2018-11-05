package master2018.flink.data;


import org.apache.flink.api.java.tuple.Tuple8;

public class CarEvent {

    int time;
    int VID;
    int speed;
    int highway;
    Lane lane;
    Direction direction;
    int segment;
    int position;

    public CarEvent(){}

    public CarEvent(int time, int VID, int speed, int highway, Lane lane, Direction direction, int segment, int position) {
        this.time = time;
        this.VID = VID;
        this.speed = speed;
        this.highway = highway;
        this.lane = lane;
        this.direction = direction;
        this.segment = segment;
        this.position = position;
    }


    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> asTuple() {
        return new Tuple8<>(
                time,
                VID,
                speed,
                highway,
                lane.ordinal(),
                direction.ordinal(),
                segment,
                position
        );
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
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

    public Lane getLane() {
        return lane;
    }

    public void setLane(Lane lane) {
        this.lane = lane;
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
