package parquet;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.json.JSONObject;

public class ParquetWriterExample {

    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(
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

        // Create the JSON object to be stored in the Parquet file
        JSONObject jsonObject = new JSONObject("{\n" +
                "    \"station_id\": 5,\n" +
                "    \"s_no\": 1,\n" +
                "    \"battery_status\": \"loww\",\n" +
                "    \"status_timestamp\": 1681521224,\n" +
                "    \"weather\": {\n" +
                "        \"humidity\": -35,\n" +
                "        \"temperature\": 100,\n" +
                "        \"wind_speed\": 13\n" +
                "    }\n" +
                "}");

        // Convert the JSON object to an Avro record
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("station_id", jsonObject.getInt("station_id"));
        avroRecord.put("s_no", jsonObject.getInt("s_no"));
        avroRecord.put("battery_status", jsonObject.getString("battery_status"));
        avroRecord.put("status_timestamp", jsonObject.getLong("status_timestamp"));

        JSONObject weatherObject = jsonObject.getJSONObject("weather");
        GenericRecord weatherRecord = new GenericData.Record(schema.getField("weather").schema());
        weatherRecord.put("humidity", weatherObject.getInt("humidity"));
        weatherRecord.put("temperature", weatherObject.getInt("temperature"));
        weatherRecord.put("wind_speed", weatherObject.getInt("wind_speed"));
        avroRecord.put("weather", weatherRecord);

        // create writer
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("example.parquet");
        CompressionCodecName codec = CompressionCodecName.SNAPPY;
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(schema)
                .withCompressionCodec(codec)
                .withDataModel(GenericData.get())
                .build();

        // write data
        writer.write(avroRecord);
        writer.write(avroRecord);

        // close writer
        writer.close();
    }

}