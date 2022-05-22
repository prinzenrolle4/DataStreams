package demo.abs4_windowed_streams;

import demo.abs1_basic_application.DemoApplication;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import java.util.Collection;
import java.util.Collections;

public class MyWindowStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        streamExecutionEnvironment.<WikipediaEditEvent>addSource(new WikipediaEditsSource())
                        .keyBy(WikipediaEditEvent::isBotEdit)
                        .window(new MyWindow(Time.seconds(5)))
                                .max("byteDiff")
                                        .print();

        streamExecutionEnvironment.execute();
    }

    private static class MyWindow extends WindowAssigner<Object, TimeWindow> {

        private final long size;
        private final long globalOffset;
        private final WindowStagger windowStagger;


        public MyWindow(Time size) {
            this.size = size.toMilliseconds();
            this.globalOffset = 0L;
            this.windowStagger = WindowStagger.ALIGNED;
        }

        @Override
        public Collection<TimeWindow> assignWindows(Object object, long l, WindowAssignerContext windowAssignerContext) {
            long now = windowAssignerContext.getCurrentProcessingTime();
            long start = getWindowStartWithOffset(now, globalOffset, size);
            return Collections.singletonList(new TimeWindow(start, start + this.size));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
            return ProcessingTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return false;
        }
        private static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
            return timestamp - (timestamp - offset + windowSize) % windowSize;
        }

    }
}
