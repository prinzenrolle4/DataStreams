package demo.basic_application;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class DemoApplication {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        /*StreamExecutionEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment(
                "host",
                1234,
                "path/to/jarFile.jar");*/

        DataStreamSource<Integer> dataStreamSource = executionEnvironment.addSource(new DemoSource());
        SingleOutputStreamOperator<Integer> newDataStream = dataStreamSource.map(value -> value + 1);
        newDataStream.print();

        executionEnvironment.execute("Demo Example");
    }

    private static class DemoSource implements SourceFunction<Integer> {

        private volatile boolean isRunning = true;
        private static Random RANDOM = new Random();

        @Override
        public void run(SourceContext<Integer> sourceContext) throws InterruptedException {
            while (isRunning) {
                int value = RANDOM.nextInt(10) + 1;
                sourceContext.collect(value);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
