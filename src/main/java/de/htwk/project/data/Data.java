package de.htwk.project.data;

import de.htwk.project.api.model.SensorData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class Data {

    private static final Random RANDOM = new Random();

    public DataStream<SensorData> getSensorData(StreamExecutionEnvironment env) {

        return env.fromElements(
                new SensorData(UUID.randomUUID(), Instant.now().plusSeconds(5).getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().plusSeconds(4).getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().plusSeconds(3).getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().plusSeconds(2).getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().plusSeconds(1).getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().plusSeconds(10).getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().plusSeconds(11).getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble()),
                new SensorData(UUID.randomUUID(), Instant.now().getEpochSecond(), RANDOM.nextDouble())
        );
    }


}
