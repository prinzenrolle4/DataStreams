package demo.abs6_process_functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DataStreamProcessFunction {
    public static void main(String[] args) throws Exception {

        final OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<>("big-edits") {};

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> process = streamExecutionEnvironment.addSource(new WikipediaEditsSource())
                .process(new MyProcessFunction(outputTag));

        DataStream<Tuple2<String, Integer>> sideOutput = process.getSideOutput(outputTag);
        sideOutput.map(event -> event.f0 + " machte eine Änderung von " + event.f1 + " Zeichen.").print();

        process.print();

        streamExecutionEnvironment.execute();
    }

    private static class MyProcessFunction extends ProcessFunction<WikipediaEditEvent, Integer> {

        private final OutputTag<Tuple2<String, Integer>> outputTag;

        public MyProcessFunction(OutputTag<Tuple2<String, Integer>> outputTag) {
            this.outputTag = outputTag;
        }

        @Override
        public void processElement(WikipediaEditEvent wikipediaEditEvent,
                                   ProcessFunction<WikipediaEditEvent, Integer>.Context context,
                                   Collector<Integer> collector) throws Exception {
            if (!wikipediaEditEvent.isBotEdit()) {
                collector.collect(wikipediaEditEvent.getByteDiff());
            }

            if (Math.abs(wikipediaEditEvent.getByteDiff()) > 500) {
                context.output(outputTag, new Tuple2<>(wikipediaEditEvent.getUser(), wikipediaEditEvent.getByteDiff()));
            }

        }
    }
}

