package utils;
import org.everit.json.schema.ValidationException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import  org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;

import java.io.FileNotFoundException;

public class JSONValidator {
    public static void main(String[] args)  {

        String jsonString = "{\n" +
                "    \"station_id\": 5,\n" +
                "    \"s_no\": 1,\n" +
                "    \"battery_status\": \"loww\",\n" +
                "    \"status_timestamp\": 1681521224,\n" +
                "    \"weather\": {\n" +
                "        \"humidity\": -35,\n" +
                "        \"temperature\": 100,\n" +
                "        \"wind_speed\": 13\n" +
                "    }\n" +
                "}" ;
        String errMsg = validate(jsonString);
        JSONObject obj = new JSONObject();
        obj.put("error_message",errMsg);
        obj.put("timestamp",System.currentTimeMillis()/1000);
        obj.put("message",new JSONObject(jsonString));
        System.out.println("current obj\n"+obj);
    }
    private static final String jsonSchema = "{\n" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"station_id\": {\n" +
            "      \"type\": \"integer\",\n" +
            "      \"format\": \"int64\"\n" +
            "    },\n" +
            "    \"s_no\": {\n" +
            "      \"type\": \"integer\",\n" +
            "      \"format\": \"int64\"\n" +
            "    },\n" +
            "    \"battery_status\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"enum\": [\n" +
            "        \"low\",\n" +
            "        \"medium\",\n" +
            "        \"high\"\n" +
            "      ]\n" +
            "    },\n" +
            "    \"status_timestamp\": {\n" +
            "      \"type\": \"integer\",\n" +
            "      \"format\": \"int64\"\n" +
            "    },\n" +
            "    \"weather\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"properties\": {\n" +
            "        \"humidity\": {\n" +
            "          \"type\": \"integer\",\n" +
            "          \"minimum\": 0,\n" +
            "          \"maximum\": 100\n" +
            "        },\n" +
            "        \"temperature\": {\n" +
            "          \"type\": \"integer\"\n" +
            "        },\n" +
            "        \"wind_speed\": {\n" +
            "          \"type\": \"integer\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"required\": [\n" +
            "        \"humidity\",\n" +
            "        \"temperature\",\n" +
            "        \"wind_speed\"\n" +
            "      ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"station_id\",\n" +
            "    \"s_no\",\n" +
            "    \"battery_status\",\n" +
            "    \"status_timestamp\",\n" +
            "    \"weather\"\n" +
            "  ]\n" +
            "}";
    private static final JSONObject schema = new JSONObject(new JSONTokener(jsonSchema));

    public static String validate(String msg){
        Schema jsonSchema = SchemaLoader.load(schema);
        try {
            JSONObject jsonObject = new JSONObject(msg);
            jsonSchema.validate(new JSONObject(jsonObject.toString()));
        }catch (ValidationException | JSONException e){

            return e.getMessage();
        }
        return null;
    }
}
