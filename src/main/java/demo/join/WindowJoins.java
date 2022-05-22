package demo.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;

//common key, in same Window
//TumblingWindowJoin, SlidingWindowJoin, SessionWindowJoin

//IntervalJoin share common key and elements in B have timestaps that lie in a relative bound from A
public class WindowJoins {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.<TimeObject>addSource(new TimeSource())
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<TimeObject>forMonotonousTimestamps()
                                .withTimestampAssigner((timeObject, l) -> timeObject.timestamp))
                        .map(timeObject -> timeObject.value)
                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                        .max(0)
                        .print();

        streamExecutionEnvironment.execute();
    }

    private static class TimeSource implements SourceFunction<TimeObject> {

        private volatile boolean isRunning = true;
        private static List<TimeObject> timeObjects = List.of(
                new TimeObject(200L, 5),
                new TimeObject(400L, 12),
                new TimeObject(800L, 14),
                new TimeObject(600L, 42),
                new TimeObject(1000L, 3),
                new TimeObject(1200L, 4),
                new TimeObject(2100L, 72)
        );


        @Override
        public void run(SourceContext<TimeObject> sourceContext) throws InterruptedException {
            int index = 0;

            while(isRunning && index < timeObjects.size()) {
                long now = System.currentTimeMillis();
                sourceContext.collect(timeObjects.get(index));
                //sourceContext.collectWithTimestamp(timeObjects.get(index), timeObjects.get(index).timestamp);
                index++;
                Thread.sleep(500L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
