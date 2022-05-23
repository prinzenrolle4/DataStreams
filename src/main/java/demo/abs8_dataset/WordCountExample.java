package demo.abs8_dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = executionEnvironment.fromElements(
                "Hallo",
                "ich habe ich ich ich",
                "viel Hunger"
        );

        dataSource
                .flatMap(new SentenceSplitter())
                .groupBy(0)
                .sum(1)
                .writeAsCsv("test", "\n", " ");

        //executionEnvironment.execute();
    }

    private static class SentenceSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) {
            String[] words = line.split(" ");
            for(String word : words) {
               collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
