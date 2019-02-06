package master2018.flink.data;


import org.apache.flink.api.java.tuple.Tuple8;

import java.util.Objects;

public class CarEvent {

    int time;
    int VID;
    int speed;
    int highway;
    Lane lane;
    int direction;
    int segment;
    int position;

    public CarEvent(){}

    public CarEvent(int time, int VID, int speed, int highway, Lane lane, int direction, int segment, int position) {
        this.time = time;
        this.VID = VID;
        this.speed = speed;
        this.highway = highway;
        this.lane = lane;
        this.direction = direction;
        this.segment = segment;
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CarEvent carEvent = (CarEvent) o;
        return time == carEvent.time &&
                VID == carEvent.VID &&
                speed == carEvent.speed &&
                highway == carEvent.highway &&
                segment == carEvent.segment &&
                position == carEvent.position &&
                direction == carEvent.direction &&
                Objects.equals(lane.name(), carEvent.lane.name());
    }

    @Override
    public int hashCode() {
        System.out.println("++++++++++++++++++++++++++++++++++++++++++TEST++++++++++++++++++++++++++");
        int result = 1;
        result = 31 * result + time;
        result = 31 * result + VID;
        result = 31 * result + speed;
        result = 31 * result + highway;
        result = 31 * result + segment;
        result = 31 * result + position;
        result = 31 * result + lane.name().hashCode();
        result = 31 * result + direction;
        return result;
    }

    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> asTuple() {
        return new Tuple8<>(
                time,
                VID,
                speed,
                highway,
                lane.ordinal(),
                direction,
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

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
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