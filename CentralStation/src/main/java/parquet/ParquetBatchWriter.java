package parquet;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import utils.FileUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.json.JSONObject;

import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetBatchWriter {
    private final int MAX_BUFFER_SIZE = 60; //TODO: modify to 10k
    private final String TIMESTAMP;
    private final String workingDir;
    private int msgCount;
    private FileSystem fs;
    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\n" +
                    "  \"type\": \"record\",\n" +
                    "  \"name\": \"StationData\",\n" +
                    "  \"fields\": [\n" +
                    "    {\"name\": \"station_id\", \"type\": \"int\"},\n" +
                    "    {\"name\": \"s_no\", \"type\": \"int\"},\n" +
                    "    {\"name\": \"battery_status\", \"type\": \"string\"},\n" +
                    "    {\"name\": \"status_timestamp\", \"type\": \"long\"},\n" +
                    "    {\n" +
                    "      \"name\": \"weather\",\n" +
                    "      \"type\": {\n" +
                    "        \"type\": \"record\",\n" +
                    "        \"name\": \"WeatherData\",\n" +
                    "        \"fields\": [\n" +
                    "          {\"name\": \"humidity\", \"type\": \"int\"},\n" +
                    "          {\"name\": \"temperature\", \"type\": \"int\"},\n" +
                    "          {\"name\": \"wind_speed\", \"type\": \"int\"}\n" +
                    "        ]\n" +
                    "      }\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}");

    private Map<Long,Map<String, List<JSONObject>>> buffer;
    public static void main(String[] args) throws IOException {

        // Create the JSON object to be stored in the Parquet file
        JSONObject jsonObject = new JSONObject("{\n" +
                "    \"station_id\": 5,\n" +
                "    \"s_no\": 1,\n" +
                "    \"battery_status\": \"loww\",\n" +
                "    \"status_timestamp\": 1681521224,\n" +
                "    \"weather\": {\n" +
                "        \"humidity\": -35.5,\n" +
                "        \"temperature\": 100,\n" +
                "        \"wind_speed\": 13\n" +
                "    }\n" +
                "}");

        // Convert the JSON object to an Avro record
        GenericRecord avroRecord = new GenericData.Record(SCHEMA);
        for (Schema.Field field : SCHEMA.getFields()) {
            String fieldName = field.name();
            Object value = jsonObject.get(fieldName);

            if (value != null) {
                if (field.schema().getType() == Schema.Type.RECORD) {
                    // Handle nested records
                    GenericRecord nestedRecord = new GenericData.Record(field.schema());
                    JSONObject nestedObject = jsonObject.getJSONObject(fieldName);

                    for (Schema.Field nestedField : field.schema().getFields()) {
                        String nestedFieldName = nestedField.name();
                        Object nestedValue = nestedObject.get(nestedFieldName);

                        if (nestedValue != null) {
                            nestedRecord.put(nestedFieldName, nestedValue);
                        }
                    }

                    avroRecord.put(fieldName, nestedRecord);
                } else {
                    avroRecord.put(fieldName, value);
                }
            }
        }

        // create writer
        Configuration conf = new Configuration();
//        conf.setBoolean("parquet.enable.dictionary", false);
//        conf.setBoolean("parquet.enable.summary-metadata", false);
        FileSystem fs = FileSystem.get(conf);
        fs.setWriteChecksum(false);
        Path path = new Path("example5.parquet");
//        CompressionCodecName codec = CompressionCodecName.SNAPPY;
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(SCHEMA)
//                .withConf(conf)
                .build();

        // write data
        writer.write(avroRecord);
        writer.write(avroRecord);

        // close writer
        writer.close();
    }
    private class WriterThread extends Thread{ //TODO: handle 2 parquet threads
        private final Map<Long, Map<String, List<JSONObject>>> batch;

        public WriterThread(Map<Long, Map<String, List<JSONObject>>> batch){
            this.batch = batch;
        }
        @Override
        public void run(){
            System.out.println("start parquet");
            for(Long key : batch.keySet()){
                Map<String, List<JSONObject>> dateMap = batch.get(key);
                String stationDir = Paths.get(workingDir, String.valueOf(key)).toString();
                FileUtils.createDirIfNotExists(stationDir);

                for (String date : dateMap.keySet()){
                    String dateDir = Paths.get(stationDir, date).toString();
                    FileUtils.createDirIfNotExists(dateDir);
                    List<JSONObject> messages = dateMap.get(date);
                    writeListToFile(dateDir, messages);
                }
            }
            System.out.println("end parquet");
        }

        private void writeListToFile(String dateDir, List<JSONObject> messages){
            List<GenericRecord> records = new ArrayList<>();
            for (JSONObject msg : messages){
                records.add(generateRecord(msg));
            }

            //write list to parquet file
            Date date = new Date(System.currentTimeMillis());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS");
            Path path = new Path(Paths.get(dateDir, sdf.format(date) + ".parquet").toString());

            try {
                ParquetWriter<GenericRecord>writer = AvroParquetWriter.<GenericRecord>builder(path)
                        .withSchema(SCHEMA)
                        .withDataModel(GenericData.get())
                        .build();

                for (GenericRecord record : records){
                    writer.write(record);
                }

                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        private GenericRecord generateRecord(JSONObject jsonObject){
            GenericRecord avroRecord = new GenericData.Record(SCHEMA);
            for (Schema.Field field : SCHEMA.getFields()) {
                String fieldName = field.name();
                Object value = jsonObject.get(fieldName);

                if (value != null) {
                    if (field.schema().getType() == Schema.Type.RECORD) {
                        // Handle nested records
                        GenericRecord nestedRecord = new GenericData.Record(field.schema());
                        JSONObject nestedObject = jsonObject.getJSONObject(fieldName);

                        for (Schema.Field nestedField : field.schema().getFields()) {
                            String nestedFieldName = nestedField.name();
                            Object nestedValue = nestedObject.get(nestedFieldName);

                            if (nestedValue != null) {
                                nestedRecord.put(nestedFieldName, nestedValue);
                            }
                        }

                        avroRecord.put(fieldName, nestedRecord);
                    } else {
                        avroRecord.put(fieldName, value);
                    }
                }
            }

            return avroRecord;
        }
    }

    public ParquetBatchWriter(String workingDir, String timestampKey){
        this.TIMESTAMP = timestampKey;

        this.workingDir = workingDir;
        FileUtils.createDirIfNotExists(workingDir);

        try {
            this.fs = FileSystem.get(new Configuration());
            this.fs.setWriteChecksum(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.buffer = new HashMap<>();
        this.msgCount = 0;
    }

    public void write(Long key, JSONObject msg){ //key = station_id
        String date = getDate(msg.getLong(TIMESTAMP));  //partition by day

        Map<String, List<JSONObject>> dateMap = this.buffer.get(key);
        if (dateMap == null){
            dateMap = new HashMap<>();
            this.buffer.put(key, dateMap);
        }

        List<JSONObject> messages = dateMap.get(date);
        if(messages == null){
            messages = new ArrayList<>();
            dateMap.put(date, messages);
        }

        messages.add(msg);
        this.msgCount ++;

        if(this.msgCount >= MAX_BUFFER_SIZE){
            WriterThread writerThread = new WriterThread(this.buffer);
            writerThread.setName("Parquet Batch Writer Thread");
            this.buffer = new HashMap<>();
            writerThread.start();
            this.msgCount = 0;
        }
    }

    private String getDate(Long timestamp){
        Date date = new Date(timestamp * 1000L); // Convert to milliseconds
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(date);
    }
}
