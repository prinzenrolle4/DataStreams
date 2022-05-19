package demo.process_functions;

import demo.data.MockData;
import demo.sources.Temperature;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Optional;

//The processFunction gives direct access and control of the Flink’s state and the Flink’s context.
//If you want to access keyed state and timers you have to apply the ProcessFunction on a keyed stream:
// “there is at most one timer per key and timestamp. If multiple timers are registered for the same timestamp, the onTimer method will be called just once.”
public class ProcessFunctions {
    public static void main(String[] args) throws Exception {

        /**
         * Link: https://developpaper.com/flinks-datastream-time-and-window-based-operator-processfunction/
         */

        HashMap<Integer, Long> map = new HashMap<>();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.fromCollection(MockData.getTemperatureMockData())
                .keyBy(Temperature::getSensorId)
                        .process(new KeyedProcessFunction<Integer, Temperature, Object>() {

                            @Override
                            public void processElement(Temperature temperature, KeyedProcessFunction<Integer, Temperature, Object>.Context context, Collector<Object> collector) throws Exception {
                                System.out.println(temperature);
                                long currentWatermark = context.timerService().currentWatermark();

                                Optional<Long> key = Optional.ofNullable(map.get(context.getCurrentKey()));

                                key
                                        .ifPresentOrElse(
                                                value -> map.put(context.getCurrentKey(), value++),
                                                () -> map.put(context.getCurrentKey(), 1L)
                                        );

                                context.timerService().registerEventTimeTimer(currentWatermark + 10000);

                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Integer, Temperature, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
                                System.out.println(ctx.getCurrentKey() + " hat Anzahl: " + map.get(ctx.getCurrentKey()));
                            }
                        }).print();

        executionEnvironment.execute();
    }
}
