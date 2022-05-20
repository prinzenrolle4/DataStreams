package demo.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Random;

public class WindowStreams {
    //Arten: TumblingWindow, SlidingWindow, SessionWindow oder GlobalWindow (non-keyed-datastreams)
    /*https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/operators/windows/*/
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();

        streamExecutionEnvironment.<Tuple2<Integer, Integer>>addSource(new DemoSource())
                .keyBy(element -> element.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<Integer, Integer>, Object, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow timeWindow, Iterable<Tuple2<Integer, Integer>> iterable, Collector<Object> collector) throws Exception {

                    }
                }).print();

        streamExecutionEnvironment.execute();

    }

    private static class MyWindowFunction implements WindowFunction<>{
        @Override
        public void apply(Object o, Window window, Iterable iterable, Collector collector) throws Exception {

        }
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
}
