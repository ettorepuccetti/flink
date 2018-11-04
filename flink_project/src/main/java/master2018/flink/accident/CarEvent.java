package master2018.flink.accident;

import master2018.flink.data.Direction;
import master2018.flink.data.Lane;

public class CarEvent {

    int time;
    int VID;
    int speed;
    int highway;
    Lane lane;
    Direction direction;
    int segment;
    int position;

    public CarEvent(int time, int VID, int speed, int highway, int lane, int direction, int segment, int position) {
        
        setLane(lane);
        setDirection(direction);
        this.time = time;
        this.VID = VID;
        this.speed = speed;
        this.highway = highway;
        this.segment = segment;
        this.position = position;
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

    public void setLane(int lane) {
        switch (lane) {
            case 0:
                this.lane = Lane.ENTRY;
                break;
            case 1:
                this.lane = Lane.TRAVEL1;
                break;
            case 2:
                this.lane = Lane.TRAVEL2;
        
            case 3:
                this.lane = Lane.TRAVEL3;
                break;
            case 4:
                this.lane = Lane.EXIT;
        }
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public void setDirection(int direction) {
        switch (direction) {
            case 0:
                this.direction = Direction.EAST;
            case 1:
                this.direction = Direction.WEST;
        }
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
