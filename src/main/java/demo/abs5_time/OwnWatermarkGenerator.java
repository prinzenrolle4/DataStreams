package demo.abs5_time;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

public class OwnWatermarkGenerator implements WatermarkGenerator<Tuple2<Integer, Integer>> {

    private final long maxTimeLag = 500L;

    @Override
    public void onEvent(Tuple2<Integer, Integer> event, long l, WatermarkOutput watermarkOutput) {

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
}
