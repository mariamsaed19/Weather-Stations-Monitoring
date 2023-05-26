package kafka;

import com.google.gson.Gson;
import java.util.Random;

public class WeatherStation {
    private static class Weather {
        int humidity;
        int temperature;
        int wind_speed;
    }
    private long station_id;
    private long s_no;
    private long status_timestamp;
    private String battery_status;
    private WeatherStation.Weather weather;

    public WeatherStation(long station_id) {
        this.station_id = station_id;
        this.s_no = 0;
        this.battery_status = "high";
        this.weather = new WeatherStation.Weather();
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

    public void setGenerationtime_ms(long generationtime_ms) {
        this.status_timestamp = generationtime_ms;
    }

    public void setBattery_status(String battery_status) {
        this.battery_status = battery_status;
    }

    public void setHumidity(int humidity) {
        weather.humidity = humidity;
    }

    public void setTemperature(int temperature) {
        weather.temperature = temperature;
    }

    public void setWindspeed(int windspeed) {
        weather.wind_speed = windspeed;
    }
    public void generateStatusMessage(){
        double d = Math.random() * 100;
        if ((d -= 10) > 0){
            setS_no(getS_no()+1);
            // create random object
            Random randomno = new Random(); // range ==> (max - min + 1) + min
            if(randomno.nextInt(100)<5) setBattery_status("full");
            else{
                int percentage = (int) d;
                if (percentage < 30)  setBattery_status("low");
                else if (percentage < 70) setBattery_status("medium");
                else if (percentage < 100) setBattery_status("high");
            }
            setGenerationtime_ms(System.currentTimeMillis());
            setTemperature(randomno.nextInt(49) - 3);
            setWindspeed(randomno.nextInt(61)+10);
            setHumidity(randomno.nextInt(111));
        }
    }
    public static void main(String[] args) throws InterruptedException {
        Gson gson = new Gson();
        WeatherStation w = new WeatherStation(7);
        while(true){
            w.generateStatusMessage();
            String json = gson.toJson(w);
            ConnectToKafka.connect(json);
            Thread.sleep(1000); // Wait for 1 second
        }
    }
}






