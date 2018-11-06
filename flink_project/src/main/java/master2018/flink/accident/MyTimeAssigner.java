package master2018.flink.accident;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import master2018.flink.data.CarEvent;

public class MyTimeAssigner extends AscendingTimestampExtractor<CarEvent> {

    @Override
    public long extractAscendingTimestamp(CarEvent element) {
        return element.getTime()*1000;
    }
}