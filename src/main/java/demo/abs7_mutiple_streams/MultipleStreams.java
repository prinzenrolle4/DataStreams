package demo.abs7_mutiple_streams;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class MultipleStreams {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringStream = streamExecutionEnvironment.fromElements();
        DataStreamSource<Integer> integerStream = streamExecutionEnvironment.fromElements();
        DataStreamSource<Integer> secondIntegerStream = StreamExecutionEnvironment.getExecutionEnvironment().fromElements();


        DataStream<Integer> union = integerStream.union(secondIntegerStream);
        JoinedStreams<Integer, Integer>.Where<Integer>.EqualTo equalTo =
                integerStream.join(secondIntegerStream).where(integer -> integer).equalTo(integer -> integer);
        ConnectedStreams<String, Integer> connectedStream = stringStream.connect(integerStream);

        connectedStream.process(new CoProcessFunction<String, Integer, Object>() {
            @Override
            public void processElement1(String s, CoProcessFunction<String, Integer, Object>.Context context, Collector<Object> collector) throws Exception {

            }

            @Override
            public void processElement2(Integer integer, CoProcessFunction<String, Integer, Object>.Context context, Collector<Object> collector) throws Exception {

            }
        });

        connectedStream.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String s) throws Exception {
                return null;
            }

            @Override
            public Object map2(Integer integer) throws Exception {
                return null;
            }
        });
    }
}
