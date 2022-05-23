package demo.sammlung;

import demo.sammlung.data.MockData;
import demo.sammlung.entity.Temperature;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Temperature> add = executionEnvironment.fromElements(
                new Temperature(2L, 1, 50.54),
                new Temperature(23L, 2, 55.554)
        );

        DataStream<Temperature> unitedStream = executionEnvironment.fromCollection(MockData.getTemperatureMockData());


        DataStream<Temperature> union = unitedStream.union(add).shuffle();
        SingleOutputStreamOperator<Double> map = union.map(Temperature::getTemperature);
        //map.
        union.print();

        DataStream<Integer> intStream = executionEnvironment.fromElements(1, 2, 3, 4, 5);
        DataStream<String> stringStream = executionEnvironment.fromElements("Test", "HUnger", "Mittag");

        ConnectedStreams<Integer, String> connect = intStream.connect(stringStream);

        //connect.map((value, text) -> text + " " + value);


        executionEnvironment.execute();
    }
}
