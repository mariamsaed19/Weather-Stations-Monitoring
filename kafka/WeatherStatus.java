//package kafka;
//
//public class WeatherStatus {
//    private static class Weather {
//        float humidity;
//        float temperature;
//        float windspeed;
//    }
//
//    private long station_id;
//    private long s_no;
//    float generationtime_ms;
//    private String battery_status;
//    private WeatherStatus.Weather current_weather;
//
//    public WeatherStatus(long station_id) {
//        this.station_id = station_id;
//        this.s_no = 0;
//        this.battery_status = "high";
//        this.current_weather = new WeatherStatus.Weather();
//    }
//
//    public long getStation_id() {
//        return station_id;
//    }
//
//    public void setStation_id(long station_id) {
//        this.station_id = station_id;
//    }
//
//    public long getS_no() {
//        return s_no;
//    }
//
//    public void setS_no(long s_no) {
//        this.s_no = s_no;
//    }
//
//    public void setGenerationtime_ms(float generationtime_ms) {
//        this.generationtime_ms = generationtime_ms;
//    }
//
//    public void setBattery_status(String battery_status) {
//        this.battery_status = battery_status;
//    }
//
//    public void setHumidity(float humidity) {
//        current_weather.humidity = humidity;
//    }
//
//    public void setTemperature(float temperature) {
//        current_weather.temperature = temperature;
//    }
//
//    public void setWindspeed(float windspeed) {
//        current_weather.windspeed = windspeed;
//    }
//}
