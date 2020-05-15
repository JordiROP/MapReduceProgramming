package SingleApplication;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class CleanUp {

    public static class LowerCaseMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable val = new LongWritable();
        private final Text json = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ;
            json.set(value.toString().toLowerCase());
            context.write(json, val);
        }
    }

    public static class RemoveLackingFieldsMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(key.toString());
                String textField = jsonObject.get("text").toString();
                JSONObject entitiesField = (JSONObject) jsonObject.get("entities");
                JSONArray hashtagsArray = (JSONArray) entitiesField.get("hashtags");

                if (!hashtagsArray.isEmpty() && !textField.equals("")) {
                    context.write(key, value);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class FieldSelectorMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
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

    public static class LanguageFilterMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
        String filter = "es";
        // String filter = "en";

        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(key.toString());
                String lang = jsonObject.get("lang").toString();
                if(filter.equals(lang)) {
                    context.write(key, value);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

}
