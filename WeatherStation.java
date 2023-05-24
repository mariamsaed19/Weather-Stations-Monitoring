package kafka;

import com.google.gson.Gson;
import java.util.Random;

public class WeatherStation {
    private static class Weather {
        float humidity;
        float temperature;
        float windspeed;
    }

    private long station_id;
    private long s_no;
    private float generationtime_ms;
    private String battery_status;
    private WeatherStation.Weather current_weather;

    public WeatherStation(long station_id) {
        this.station_id = station_id;
        this.s_no = 0;
        this.battery_status = "high";
        this.current_weather = new WeatherStation.Weather();
    }

    public void setStation_id(long station_id) {
        this.station_id = station_id;
    }

    public long getS_no() {
        return s_no;
    }

    public void setS_no(long s_no) {
        this.s_no = s_no;
    }

    public void setGenerationtime_ms(float generationtime_ms) {
        this.generationtime_ms = generationtime_ms;
    }

    public void setBattery_status(String battery_status) {
        this.battery_status = battery_status;
    }

    public void setHumidity(float humidity) {
        current_weather.humidity = humidity;
    }

    public void setTemperature(float temperature) {
        current_weather.temperature = temperature;
    }

    public void setWindspeed(float windspeed) {
        current_weather.windspeed = windspeed;
    }
    public void generateStatusMessage(){
        double d = Math.random() * 100;
        if ((d -= 10) > 0){
            setS_no(getS_no()+1);

            // create random object
            Random randomno = new Random();

            if ((d -= 30) < 0)  setBattery_status("low");
            else if ((d -= 40) < 0) setBattery_status("medium");
            else setBattery_status("high");

            setGenerationtime_ms(System.currentTimeMillis() / 1000f);
            setTemperature(randomno.nextFloat() * 100);
            setWindspeed(randomno.nextFloat() * 100);
            setHumidity(randomno.nextFloat() * 100);
        }
    }
    public static void main(String[] args) throws InterruptedException {
        Gson gson = new Gson();
        WeatherStation w = new WeatherStation(10);
        while(true){
            w.generateStatusMessage();
            String json = gson.toJson(w);
            ConnectToKafka.connect(json);
            Thread.sleep(1000); // Wait for 1 second
        }
    }
}






