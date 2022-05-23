package demo.abs3_keyed_streams;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedExample {
    public static void main(String[] args) throws Exception {
        /*
            Berechnen der durchschnittlichen Zeit, welche Nutzer auf einer bestimmten Webseite verbringen.
         */


        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        streamExecutionEnvironment.addSource(new KeyedExampleSource())
                        .<Tuple3<String, Double, Integer>>map(new StringSplitter())
                        .keyBy(event -> event.f0)
                        .reduce(new MyReduceFunction())
                        .map(new AverageMap())
                        .print();


        streamExecutionEnvironment.execute();
    }

    private static class StringSplitter implements MapFunction<String, Tuple3<String, Double, Integer>> {
        @Override
        public Tuple3<String, Double, Integer> map(String s) throws Exception {
            String[] subStrings = s.split(";");
            return new Tuple3<>(
                    subStrings[0],
                    Double.parseDouble(subStrings[1]),
                    1
            );
        }
    }

    private static class MyReduceFunction implements ReduceFunction<Tuple3<String, Double, Integer>> {
        @Override
        public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> acc,
                                                      Tuple3<String, Double, Integer> newEvent) {
            return new Tuple3<>(acc.f0, acc.f1 + newEvent.f1, acc.f2 + newEvent.f2);
        }
    }

    private static class AverageMap implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> event) throws Exception {
            return new Tuple2<>(event.f0, event.f1/event.f2);
        }
    }
}
