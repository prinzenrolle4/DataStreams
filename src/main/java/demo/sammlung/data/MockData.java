package demo.sammlung.data;

import demo.sammlung.entity.Temperature;

import java.util.List;

public class MockData {

    private static List<Temperature> temperatures = List.of(
            new Temperature(10L, 1, 19.23),
            new Temperature(1035L, 2, 20.327),
            new Temperature(543L, 1, 19.25),
            new Temperature(1521L, 2, 20.11),
            new Temperature(10L, 3, 27.113),
            new Temperature(12L, 3, 27.32),
            new Temperature(13L,1, 19.37),
            new Temperature(14L, 2, 20.56),
            new Temperature(13L,2, 20.11),
            new Temperature(12L, 1, 19.23),
            new Temperature(12L, 3, 27.325)
    );

    public static List<Temperature> getTemperatureMockData() {
        return temperatures;
    }
}
