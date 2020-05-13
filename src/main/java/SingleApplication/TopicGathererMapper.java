package SingleApplication;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class TopicGathererMapper extends Mapper<Text, LongWritable,Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(key.toString());
            System.out.println(jsonObject.toJSONString());
            JSONArray hashtagsArray = (JSONArray) jsonObject.get("hashtags");

            for (Object jsonArrayElem : hashtagsArray) {
                if (jsonArrayElem instanceof JSONObject) {
                    String tweet = ((JSONObject) jsonArrayElem).get("text").toString();
                    word.set(tweet);
                    context.write(word, one);
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
