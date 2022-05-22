package demo.entity;

import java.io.Serializable;

public class Temperature implements Serializable {
    private long timestamp;
    private int sensorId;
    private double temperature;

    public Temperature() {
    }

    public Temperature(Long timestamp, int sensorId, double temperature) {
        this.timestamp = timestamp;
        this.sensorId = sensorId;
        this.temperature = temperature;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public int getSensorId() {
        return sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    @Override
    public String toString() {
        return "[sensorId = " + sensorId + ", Zeitstempel: " + timestamp + ", Temperatur " + temperature + "]";
    }
}
