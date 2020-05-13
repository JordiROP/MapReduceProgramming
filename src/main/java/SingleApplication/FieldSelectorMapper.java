package SingleApplication;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class FieldSelectorMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    private final Text word = new Text();

    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(key.toString());
            String text = jsonObject.get("text").toString();
            JSONObject entitiesJSONObj = (JSONObject) jsonObject.get("entities");
            JSONArray hashtagsJSONArray = (JSONArray) entitiesJSONObj.get("hashtags");
            String lang = jsonObject.get("lang").toString();

            JSONObject newJsonObject = new JSONObject();
            newJsonObject.put("text", text);
            newJsonObject.put("hashtags", hashtagsJSONArray);
            newJsonObject.put("lang", lang);

            word.set(newJsonObject.toJSONString());
            context.write(word, value);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}