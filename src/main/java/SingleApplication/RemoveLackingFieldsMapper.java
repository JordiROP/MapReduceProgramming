package SingleApplication;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class RemoveLackingFieldsMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(key.toString());
            String textField = jsonObject.get("text").toString();
            JSONObject entitiesField = (JSONObject) jsonObject.get("entities");
            JSONArray hashtagsArray = (JSONArray) entitiesField.get("hashtags");

            if(!hashtagsArray.isEmpty() && !textField.equals("")) {
                context.write(key, value);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
