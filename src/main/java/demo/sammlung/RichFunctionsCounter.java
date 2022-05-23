package demo.sammlung;

import demo.sammlung.data.MockData;
import demo.sammlung.entity.Temperature;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionsCounter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> filteredInput =
                streamExecutionEnvironment.fromCollection(MockData.getTemperatureMockData())
                    .filter(new MyRichFilterFunction())
                            .map(new MyRichMapFunction());

        filteredInput.print();
        JobExecutionResult result = filteredInput.getExecutionEnvironment().execute();

        System.out.println("'home' - Counter: " + result.getAccumulatorResult("count"));
        System.out.println("Histogram: " + result.getAccumulatorResult("temperatures"));
    }

    private static class MyRichFilterFunction extends RichFilterFunction<Temperature> {

        private final IntCounter count = new IntCounter();

        @Override
        public boolean filter(Temperature t) {
            if (t.getTemperature() > 20.0) {
                count.add(1);
                return true;
            }
            return false;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // add accumulator to runtime context with the name count
            getRuntimeContext().addAccumulator("count", this.count);
        }
    }

    private static class MyRichMapFunction extends RichMapFunction<Temperature, Integer> {

        private final Histogram temperatureHistogram = new Histogram();

        @Override
        public Integer map(Temperature t){
            int temperature = (int) t.getTemperature();
            temperatureHistogram.add(temperature);
            return temperature;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator("temperatures", this.temperatureHistogram);
        }
    }
}
