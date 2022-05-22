package demo.abs2_basic_transformations;

import demo.data.MockData;
import demo.entity.Temperature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment
                .fromCollection(MockData.getTemperatureMockData())
                .flatMap(new FlatMapFunction<Temperature, Temperature>() {
                    @Override
                    public void flatMap(Temperature temperature, Collector<Temperature> collector){
                        if (temperature.getTemperature() > 20.0) {
                            collector.collect(temperature);
                        }
                    }
                })
                .flatMap(new FlatMapFunction<Temperature, Tuple3<Long, Integer, Double>>() {
                    @Override
                    public void flatMap(Temperature temperature, Collector<Tuple3<Long, Integer, Double>> collector) {
                        Tuple3<Long, Integer, Double> temperatureTuple = new Tuple3<>(
                                temperature.getTimestamp(),
                                temperature.getSensorId(),
                                BigDecimal
                                        .valueOf(temperature.getTemperature())
                                        .setScale(2, RoundingMode.HALF_UP)
                                        .doubleValue());
                        collector.collect(temperatureTuple);
                    }
                })
                .print();

        executionEnvironment.execute();
    }
}
