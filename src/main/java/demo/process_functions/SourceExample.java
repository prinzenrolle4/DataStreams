package demo.process_functions;

import demo.sources.RandomFibonacciSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class SourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment
                .addSource(new RandomFibonacciSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)))
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> element) throws Exception {
                        return new Tuple2<>(element.f0 - 1, element.f1 + 1);
                    }
                })
                .keyBy(element -> element.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .max(1)
                .print();

        executionEnvironment.execute();
    }
}
