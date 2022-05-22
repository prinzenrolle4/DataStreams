package demo.abs4_windowed_streams;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Random;

public class WindowStreams {
    //Arten: TumblingWindow, SlidingWindow, SessionWindow oder GlobalWindow
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();

        streamExecutionEnvironment.<Tuple2<Integer, Integer>>addSource(new DemoSource())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();

        streamExecutionEnvironment.<Tuple2<Integer, Integer>>addSource(new DemoSource())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AverageAggregate())
                .map(value -> "Durchschnitt: " + value)
                .print();

        streamExecutionEnvironment.<Tuple2<Integer, Integer>>addSource(new DemoSource())
                        .keyBy(value -> value.f0)
                        .window(GlobalWindows.create())
                        .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                        .sum(1)
                        .print();

        streamExecutionEnvironment.execute();

    }

    private static class DemoSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private volatile boolean isRunning = true;
        private static Random RANDOM = new Random();

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> sourceContext) throws InterruptedException {
            while (isRunning) {
                int value = RANDOM.nextInt(10) + 1;
                int key = RANDOM.nextInt(3) + 1;
                sourceContext.collect(new Tuple2<>(key, value));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    // <IN, ACC, OUT>
    private static class AverageAggregate implements AggregateFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Integer> {
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0,0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Tuple2<Integer, Integer> newValue, Tuple2<Integer, Integer> acc) {
            return new Tuple2<>(acc.f0 + newValue.f1, acc.f1 + 1);
        }

        @Override
        public Integer getResult(Tuple2<Integer, Integer> acc) {
            return acc.f0 / acc.f1;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc1, Tuple2<Integer, Integer> acc2) {
            return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }
}
