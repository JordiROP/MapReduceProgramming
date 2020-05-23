package SingleApplicationUpgrades;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class CleanUp {

    public static class LowerCaseMapper extends Mapper<Object, Text, IntWritable, CustomTweetWritable> {
        private final static IntWritable val = new IntWritable(1);
        CustomTweetWritable ctw = new CustomTweetWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ctw.setCounter(val);
            ctw.setTweet(new Text(value.toString().toLowerCase()));

            context.write(val, ctw);
        }
    }

    public static class RemoveLackingFieldsMapper extends Mapper<IntWritable, CustomTweetWritable, IntWritable, CustomTweetWritable> {
        public void map(IntWritable key, CustomTweetWritable value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(value.getTweet().toString());
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

    public static class FieldSelectorMapper extends Mapper<IntWritable, CustomTweetWritable, IntWritable, CustomTweetWritable> {
        public void map(IntWritable key, CustomTweetWritable value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(value.getTweet().toString());
                String text = jsonObject.get("text").toString();
                JSONObject entitiesJSONObj = (JSONObject) jsonObject.get("entities");
                JSONArray hashtagsJSONArray = (JSONArray) entitiesJSONObj.get("hashtags");
                String lang = jsonObject.get("lang").toString();

                JSONObject newJsonObject = new JSONObject();
                newJsonObject.put("text", text);
                newJsonObject.put("hashtags", hashtagsJSONArray);
                newJsonObject.put("lang", lang);

                value.setTweet(new Text(newJsonObject.toJSONString()));
                context.write(key, value);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LanguageFilterMapper extends Mapper<IntWritable, CustomTweetWritable, IntWritable, CustomTweetWritable> {
        public void map(IntWritable key, CustomTweetWritable value, Context context) throws IOException, InterruptedException {
            String filter  = context.getConfiguration().get("lang");
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(value.getTweet().toString());
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
