package demo.abs6_process_functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.util.Collector;

public class KeyedStreamProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.<WikipediaEditEvent>addSource(new WikipediaEditsSource())
                .keyBy(WikipediaEditEvent::isBotEdit)
                .process(new MyProcessFunction())
                .print();

        streamExecutionEnvironment.execute();
    }

    private static class CountSumWithTimeStamp {
        public boolean key;
        public int count;
        public double sum;
        public Long lastTimestamp;
    }

    private static class MyProcessFunction extends KeyedProcessFunction<Boolean, WikipediaEditEvent, String> {

        private transient ValueState<CountSumWithTimeStamp> countSumWithTimeStampValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countSumWithTimeStampValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<CountSumWithTimeStamp>(
                            "countSumWithTimestampState",
                            CountSumWithTimeStamp.class
                    ));
        }

        @Override
        public void processElement(WikipediaEditEvent wikipediaEditEvent,
                                   KeyedProcessFunction<Boolean, WikipediaEditEvent, String>.Context context,
                                   Collector<String> collector) throws Exception {

            CountSumWithTimeStamp current = countSumWithTimeStampValueState.value();

            if (current == null) {
                current = new CountSumWithTimeStamp();
                current.key = wikipediaEditEvent.isBotEdit();
            }

            current.count++;
            current.sum += Math.abs(wikipediaEditEvent.getByteDiff());
            current.lastTimestamp = wikipediaEditEvent.getTimestamp();

            countSumWithTimeStampValueState.update(current);

            if (Math.abs(wikipediaEditEvent.getByteDiff()) > 500) {
                context.timerService().registerProcessingTimeTimer(
                        current.lastTimestamp + 5000);
            }

        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Boolean,
                            WikipediaEditEvent, String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {

            CountSumWithTimeStamp value = countSumWithTimeStampValueState.value();

            out.collect("Meldung nach 5 Sekunden: Es wurde eine große Änderung getätigt: " + value.sum/ value.count +
                    "\n Bot?: " + value.key + " Anzahl der Änderungen: " + value.count);

        }
    }
}
