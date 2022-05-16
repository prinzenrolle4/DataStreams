package demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

public class MapToTuple3Function implements MapFunction<WikipediaEditEvent, Tuple3<String, Integer, Long>> {

    @Override
    public Tuple3<String, Integer, Long> map(WikipediaEditEvent wikipediaEditEvent) {
        return new Tuple3<>(
                wikipediaEditEvent.getUser(),
                wikipediaEditEvent.getByteDiff(),
                wikipediaEditEvent.getTimestamp()
        );
    }
}
