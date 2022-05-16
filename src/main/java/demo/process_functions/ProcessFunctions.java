package demo.process_functions;

import demo.data.MockData;
import demo.sources.Temperature;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

//The processFunction gives direct access and control of the Flink’s state and the Flink’s context.
public class ProcessFunctions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.fromCollection(MockData.getTemperatureMockData())
                .keyBy(Temperature::getSensorId)
                        .process(new KeyedProcessFunction<Integer, Temperature, Object>() {

                            @Override
                            public void processElement(Temperature temperature, KeyedProcessFunction<Integer, Temperature, Object>.Context context, Collector<Object> collector) throws Exception {
                                System.out.println(temperature);
                                long timer = 0;
                                long currentWatermark = context.timerService().currentWatermark();

                                if (temperature.getTimestamp() % 2 == 0) {

                                    timer = currentWatermark + 3;

                                } else {

                                    timer = currentWatermark + 10;

                                }
                                context.timerService().registerEventTimeTimer(timer);

                                System.out.println("Current watermark " + currentWatermark);

                                System.out.println("Registered timer on " + timer);

                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Integer, Temperature, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
                                System.out.println("Timer registered at " + timestamp + " has fired");
                            }
                        }).print();

        executionEnvironment.execute();
    }
}
