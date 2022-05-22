package demo.abs5_time;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomIntegerTupleSource implements SourceFunction<Tuple2<Integer, Integer>> {
    private static final long serialVersionUID = 1L;

    private static final int BOUND = 100;
    private Random rnd = new Random();

    private volatile boolean isRunning = true;
    private int counter = 0;

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

        long startValue = System.currentTimeMillis();

        while (isRunning && counter < BOUND) {
            int first = rnd.nextInt(BOUND / 2 - 1) + 1;
            int second = rnd.nextInt(BOUND / 2 - 1) + 1;

            // Event wird direkt mit einem Timestamp versehen und an die Pipline geschickt
            ctx.collectWithTimestamp(new Tuple2<>(first, second), startValue + rnd.nextInt(10000));
            counter++;
            Thread.sleep(50L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}