package demo.sources;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TemperatureDataSource extends RichSourceFunction<Temperature> implements Serializable {
    private boolean isRunning = true;

    List<Temperature> temperatures = new ArrayList<>();

    @Override
    public void run(SourceContext sourceContext) throws Exception {

        //long startTime = System.currentTimeMillis();
        for (int i = 0; i < temperatures.size() && isRunning; i++) {
            Temperature temperature = temperatures.get(i);
            sourceContext.collect(temperature);
            //Thread.sleep(100);
        }
        Thread.sleep(500);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
