package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class RainDetector {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");

        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream from the input topic
        KStream<String, String> source = builder.stream("weather-status");


        // Filter the messages based on humidity value and create a new KStream with filtered messages
        KStream<String, String> filtered = source.filter((key, value) -> {
            JSONObject message = new JSONObject(value);
            double humidity = message.getJSONObject("current_weather").getDouble("humidity");
            return humidity > 70;
        });
        // Extract the station id from the filtered messages and send it to the output topic
        filtered.foreach((key, value) -> {
            JSONObject message = new JSONObject (value);
            long stationId = message.getLong("station_id");
            float time = message.getFloat("generationtime_ms");
            KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(props);
            kafkaProducer.send(new ProducerRecord<>("rain-alert", "Station "+ Long.toString(stationId)
                    + " has high humidity at " + Float.toString(time)));
        });

        // Start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }
}
