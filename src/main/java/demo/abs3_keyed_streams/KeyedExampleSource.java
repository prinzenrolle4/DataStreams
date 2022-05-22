package demo.abs3_keyed_streams;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Random;

public class KeyedExampleSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;
    private static final Random RANDOM = new Random();
    private static final List<String> websites = List.of("home", "sales", "user-profile");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        String event;
        while (isRunning) {

            event = websites.get(RANDOM.nextInt(3));
            event += ";";
            event += RANDOM.nextDouble(10.0);

            ctx.collect(event);
            Thread.sleep(1500);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}