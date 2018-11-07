package master2018.flink.data;


import org.apache.flink.api.java.tuple.Tuple8;

public class CarEvent<K> extends Tuple8<K,K,K,K,K,K,K,K> {

    public CarEvent(){}
    public CarEvent(K time, K VID, K speed, K highway, K lane, K direction, K segment, K position) {
        this.f0 = time;
        this.f1 = VID;
        this.f2 = speed;
        this.f3 = highway;
        this.f4 = lane;
        this.f5 = direction;
        this.f6 = segment;
        this.f7 = position;
    }

    public K getTime() {
        return f0;
    }

    public void setTime(K time) {
        this.f0 = time;
    }

    public K getVID() {
        return f1;
    }

    public void setVID(K VID) {
        this.f1 = VID;
    }

    public K getSpeed() {
        return f2;
    }

    public void setSpeed(K speed) {
        this.f2 = speed;
    }

    public K getHighway() {
        return f3;
    }

    public void setHighway(K highway) {
        this.f3 = highway;
    }

    public K getLane() {
        return f4;
    }

    public void setLane(K lane) {
        this.f4 = lane;
    }

    public K getDirection() {
        return f5;
    }

    public void setDirection(K direction) {
        this.f5 = direction;
    }

    public K getSegment() {
        return f6;
    }

    public void setSegment(K segment) {
        this.f6 = segment;
    }

    public K getPosition() {
        return f7;
    }

    public void setPosition(K position) {
        this.f7 = position;
    }
}