package demo.abs3_keyed_streams;

import demo.data.MockData;
import demo.entity.Temperature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //mögliche Funktionen: max(), min(), minBy(), maxBy(), sum(), reduce()
        // rolling aggregations nur bei endlicher Anzahl an Schlüsseln,
        // da sonst für jeden Key ein State gespeichert wird und diese nie gelöscht werden

        executionEnvironment.fromCollection(MockData.getTemperatureMockData())
                .map(new ToTuple3Mapper())
                .keyBy(element -> element.f1)
                .reduce((x, y) -> (new Tuple3<>(x.f0, x.f1, Math.max(x.f2, y.f2))))
                .print();

        executionEnvironment.execute();
    }

    private static class Reduce implements ReduceFunction<Tuple3<Long, Integer, Double>> {
        @Override
        public Tuple3<Long, Integer, Double> reduce(Tuple3<Long, Integer, Double> longIntegerDoubleTuple3, Tuple3<Long, Integer, Double> t1) throws Exception {
            return null;
        }
    }

    private static class ToTuple3Mapper implements MapFunction<Temperature, Tuple3<Long, Integer, Double>> {
        @Override
        public Tuple3<Long, Integer, Double> map(Temperature temperature) throws Exception {
            return new Tuple3<>(
                    temperature.getTimestamp(),
                    temperature.getSensorId(),
                    temperature.getTemperature());
        }
    }
}
