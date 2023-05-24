package kafka;

import java.io.IOException;
import java.util.Random;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.http.GET;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.Query;

public class OpenMeteoWeatherStation {

    // Define the Open-Meteo API base URL
    private static final String API_BASE_URL = "https://api.open-meteo.com/";

    // Define the Retrofit service interface for the Open-Meteo API
    private interface OpenMeteoService {
        @GET("v1/forecast")
        Call<WeatherStation> getWeatherData(@Query("latitude") double latitude,
                                           @Query("longitude") double longitude,
                                           @Query("current_weather") boolean currentWeather);
    }

    public WeatherStation constructStation(long id) {
        // Create Retrofit instance to make API requests
        Gson gson = new GsonBuilder().create();
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(API_BASE_URL)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .build();
        OpenMeteoService openMeteoService = retrofit.create(OpenMeteoService.class);
        Random randomno = new Random();
        double latitude = 50 + randomno.nextFloat() * 10;
        double longitude = 15 + randomno.nextFloat() * 10;
        // Retrieve weather data from Open-Meteo API and send to Kafka queue
        Call<WeatherStation> call = openMeteoService.getWeatherData(latitude,longitude,true);
        WeatherStation weatherData = new WeatherStation(id);

        try {
            weatherData = call.execute().body();
        } catch (IOException e) { // Handle API request error
            e.printStackTrace();
        }
        // missing attributes
        weatherData.setStation_id(id);
        double d = Math.random() * 100;
        if ((d -= 30) < 0) weatherData.setBattery_status("low");
        else if ((d -= 40) < 0) weatherData.setBattery_status("medium");
        else weatherData.setBattery_status("high");
        float humidity = randomno.nextFloat() * 100;
        weatherData.setHumidity(humidity);

        return weatherData;
    }

    public static void main(String[] args) throws InterruptedException {
        Gson gson = new Gson();
        OpenMeteoWeatherStation adapter = new OpenMeteoWeatherStation();
        long seq = 1;
        while(true){
            WeatherStation weatherData = adapter.constructStation(5);
            weatherData.setS_no(seq);
            String json = gson.toJson(weatherData);
            ConnectToKafka.connect(json);
            Thread.sleep(1000); // Wait for 1 second
            seq++;
        }
    }

}
