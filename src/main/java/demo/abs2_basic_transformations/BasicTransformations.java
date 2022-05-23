package demo.abs2_basic_transformations;

import demo.sammlung.data.MockData;
import demo.sammlung.entity.Temperature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class BasicTransformations {
    public static void main(String[] args) throws Exception {

        // create environment
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();

        // simulate input
        DataStreamSource<Temperature> temperatureDataStreamSource = localEnvironment
                .fromCollection(MockData.getTemperatureMockData());

        // filter unneeded data
        SingleOutputStreamOperator<Temperature> filterTemperatureData = temperatureDataStreamSource
                        .filter(temperature -> temperature.getTemperature() > 20.0);

        // map to new datatype and set precision
        SingleOutputStreamOperator<Tuple3<Long, Integer, Double>> output = filterTemperatureData.map(new MyMapFunction());

        // print result
        output.print();

        localEnvironment.execute();

    }

    public static class MyMapFunction implements MapFunction<Temperature, Tuple3<Long, Integer, Double>> {
        @Override
        public Tuple3<Long, Integer, Double> map(Temperature temperature) {
            return new Tuple3<>(
                    temperature.getTimestamp(),
                    temperature.getSensorId(),
                    BigDecimal
                            .valueOf(temperature.getTemperature())
                            .setScale(2, RoundingMode.HALF_UP)
                            .doubleValue());
        }
    }
}
