package demo.windowing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class SplitMap implements MapFunction<SiemEvent, Tuple3<Integer, Integer, String>> {

    @Override
    public Tuple3<Integer, Integer, String> map(SiemEvent siemEvent) {
        return new Tuple3<>(1, siemEvent.value, siemEvent.eventTime + ", ");
    }
}
