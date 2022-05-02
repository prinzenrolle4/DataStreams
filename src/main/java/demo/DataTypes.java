package demo;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.operators.StreamProject;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class DataTypes<T extends Window, U extends Tuple> {
    DataStream<T> dataStream;
    SingleOutputStreamOperator<T> singleOutputStreamOperator;
    WindowedStream<T, T, T> windowedStream;
    KeyedStream<T, String> keyedStream;
    StreamProjection<T> streamProjection;
    StreamProject<T, U> streamProject;
}
