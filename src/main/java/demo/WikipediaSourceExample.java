package demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import java.time.Duration;

public class WikipediaSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.addSource(new WikipediaEditsSource())
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                    .<WikipediaEditEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                    .withTimestampAssigner((wikipediaEditEvent, l) -> wikipediaEditEvent.getTimestamp()))
                                .map(new MapToTuple2Function())
                                .keyBy(element -> element.f0)
                                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                                .sum(1)
                                .print();

        executionEnvironment.execute();
    }

    private static class MapToTuple2Function implements MapFunction<WikipediaEditEvent, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(WikipediaEditEvent wikipediaEditEvent) throws Exception {
            return new Tuple2<String, Integer>(
                    wikipediaEditEvent.getUser(),
                    wikipediaEditEvent.getByteDiff()
            );
        }
    }
}
