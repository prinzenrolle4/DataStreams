package demo.abs5_time;

import demo.abs6_process_functions.KeyedStreamProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntegerTupleSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment
                .addSource(new RandomIntegerTupleSource())
                // Definieren der Watermark Strategie
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2)))
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> element) throws Exception {
                        return new Tuple2<>(element.f0 - 1, element.f1 + 1);
                    }
                })
                .keyBy(element -> element.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .process(new OwnWindowProcessFunction())
                .print();

        executionEnvironment.execute();
    }

    //<IN, OUT, KEY, W>
    private static class OwnWindowProcessFunction extends ProcessWindowFunction<Tuple2<Integer, Integer>, String, Integer, TimeWindow> {


        @Override
        public void process(Integer key,
                            ProcessWindowFunction<Tuple2<Integer, Integer>, String, Integer, TimeWindow>.Context context,
                            Iterable<Tuple2<Integer, Integer>> events,
                            Collector<String> collector) throws Exception {
                Long currentWatermark = context.currentWatermark();
                Long startOfWindow = context.window().getStart();
                Long endOfWindow = context.window().getEnd();


        }
    }
}
