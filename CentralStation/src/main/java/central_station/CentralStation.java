package central_station;

import bitcask.Bitcask;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import parquet.ParquetBatchWriter;
import utils.JSONValidator;

import java.util.Collections;
import java.util.Properties;

/*
    - poll from kafka
    - insert into bitcask
    - buffer batch for parquet
    - spawn thread to write parquet

 */
public class CentralStation {
    public static final String topicName = "weather-status";
    public static final  String invalidChannel = "invalid-messages";
    private Properties consumerProps, producerProps;

    private  KafkaConsumer<String, String> kafkaConsumer;
    private  KafkaProducer<String, String> kafkaProducer ;

    private final String KEY_NAME = "station_id";
    private final String TIMESTAMP = "status_timestamp";
    private static final String BITCASK_WORKING_DIR = "bitcask";
    private Bitcask bitcask;

    private static final String PARQUET_WORKING_DIR = "parquet";
    private ParquetBatchWriter parquetWriter;
    public CentralStation(){
        initializeProperties();

        kafkaConsumer = new KafkaConsumer<>(consumerProps);
        kafkaConsumer.subscribe(Collections.singleton(topicName));
        kafkaProducer = new KafkaProducer<>(producerProps);

        this.bitcask = new Bitcask(BITCASK_WORKING_DIR, KEY_NAME);
        this.parquetWriter = new ParquetBatchWriter(PARQUET_WORKING_DIR, TIMESTAMP);
    }
    private void initializeProperties(){
        // consumer configuration
        consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // producer configuration
        producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static void main(String[] args){
        CentralStation station = new CentralStation();
        station.startStation();
    }

    public void startStation(){
        while (true) {
            // poll messages from kafka
            ConsumerRecords<String, String> records = this.kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                String msg = record.value();
                System.out.println(msg);

                // validate message
                String errMsg = JSONValidator.validate(msg);
                if(errMsg == null) { // message is valid
                    JSONObject jsonMsg = new JSONObject(msg);
                    // update bitcask
                    this.bitcask.update(jsonMsg);
                    // add to batch and check batch size and spawn thread for writing
                    this.parquetWriter.write(jsonMsg.getLong(KEY_NAME), jsonMsg);
                } else{
                    System.out.println("Send to invalid channel ... :" + errMsg);
                    // wrap message
                    String wrappedMsg = wrapMessage(msg, errMsg);
                    // add to invalid channel
                    ProducerRecord<String, String> invalid_record = new ProducerRecord<>(invalidChannel, wrappedMsg);
                    this.kafkaProducer.send(invalid_record);
                }
            }
        }
    }

    private static String wrapMessage(String msg, String errMsg){
        JSONObject wrappedMsg = new JSONObject();
        wrappedMsg.put("error_message",errMsg);
        wrappedMsg.put("timestamp",System.currentTimeMillis()/1000);
        wrappedMsg.put("message",msg);
        return wrappedMsg.toString();
    }
}
