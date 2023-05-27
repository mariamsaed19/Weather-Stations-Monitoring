package utils;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestWord;
import de.vandermeer.asciithemes.a7.A7_Grids;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import org.json.JSONObject;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrintUtils {
    /*
    public static void main(String[] args) throws Exception {
        /*
        AsciiTable table = new AsciiTable();
        table.addRule();
        List<String> x = new ArrayList<>();
        x.add("John Doe");
        x.add("35");
        x.add("software");
        table.addRow(x);
        table.addRule();
        table.addRow(x);

        table.addRule();

        String renderedTable = table.render();
        System.out.println(renderedTable);

        String jsonStr ="{\n" +
                "    \"station_id\": 1,\n" +
                "    \"s_no\": 1,\n" +
                "    \"battery_status\": \"low\",\n" +
                "    \"status_timestamp\": 1681521224,\n" +
                "    \"weather\": {\n" +
                "        \"humidity\": 35,\n" +
                "        \"temperature\": 100,\n" +
                "        \"wind_speed\": 13\n" +
                "    }\n" +
                "}" ;

        JSONObject jsonObj = new JSONObject(jsonStr);

        //loopJsonObject(jsonObj, "");
        Map<String, Object> fields = getJsonFields(jsonObj);

        for (String key : fields.keySet()) {
            System.out.println(key + ": " + fields.get(key));
        }


    }
    */
    public static void showTable(List<List<String>> rows){
        AsciiTable table = new AsciiTable();

        table.addRule();
        for(List<String> row: rows){
            table.addRow(row);
            table.addRule();
        }
        if(rows.size()==0){
            table.addRow("empty");
            table.addRule();
        }

        table.setTextAlignment(TextAlignment.CENTER);
        table.getRenderer().setCWC(new CWC_LongestWord());


        String renderedTable = table.render();
        System.out.println(renderedTable);
    }
    public static Map<String, Object> getJsonFields(JSONObject jsonObject) {
        Map<String, Object> fields = new HashMap<>();

        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);

            if (value instanceof JSONObject) {
                fields.putAll(getJsonFields((JSONObject) value));
            } else if (value instanceof JSONArray) {
                // handle arrays as needed
            } else {
                fields.put(key, value);
            }
        }

        return fields;
    }
    private static void loopJsonObject(JSONObject obj, String prefix) {
        for (String key : obj.keySet()) {
            Object value = obj.get(key);
            if (value instanceof JSONObject) {
                loopJsonObject((JSONObject) value, prefix + key + ".");
            } else if (value instanceof JSONArray) {
                loopJsonArray((JSONArray) value, prefix + key + ".");
            } else {
                System.out.println(prefix + key + ": " + value.toString());
            }
        }
    }

    private static void loopJsonArray(JSONArray arr, String prefix) {
        for (int i = 0; i < arr.length(); i++) {
            Object value = arr.get(i);
            if (value instanceof JSONObject) {
                loopJsonObject((JSONObject) value, prefix + i + ".");
            } else if (value instanceof JSONArray) {
                loopJsonArray((JSONArray) value, prefix + i + ".");
            } else {
                System.out.println(prefix + i + ": " + value.toString());
            }
        }
    }
}