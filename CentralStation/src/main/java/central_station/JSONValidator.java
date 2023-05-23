package central_station;
import org.everit.json.schema.ValidationException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
public class JSONValidator {
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

    public static boolean validate(JSONObject jsonObject){
        org.everit.json.schema.Schema jsonSchema = org.everit.json.schema.loader.SchemaLoader.load(schema);
        try {
            jsonSchema.validate(new JSONObject(jsonObject.toString()));
        }catch (ValidationException | JSONException e){
            return false;
        }
        return true;
    }
}
