package demo.windowing;

import demo.MapToTuple3Function;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikiEditsEvents {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> dataStream = env.addSource(new WikipediaEditsSource());
        dataStream
                //.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)))
                .map(new MapToTuple3Function())
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((t1,t2) -> {
                    t1.f1 += t2.f1;
                    return t1;})
                .print();



        // execute program
        env.execute("WikiEdits");
    }
}
