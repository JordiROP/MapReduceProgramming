package SingleApplicationUpgrades.SingleApplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class TrendingTopics {

    public static class TrendingTopicMapper extends Mapper<Object, CustomTweetWrittable, Text, IntWritable> {
        private final Text word = new Text();

        public void map(Object key, CustomTweetWrittable value, Context context) throws IOException, InterruptedException {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(value.getTweet().toString());
                JSONArray hashtagsArray = (JSONArray) jsonObject.get("hashtags");

                for (Object jsonArrayElem : hashtagsArray) {
                    if (jsonArrayElem instanceof JSONObject) {
                        String tweet = ((JSONObject) jsonArrayElem).get("text").toString();
                        word.set(tweet);
                        context.write(word, value.getCounter());
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class TrendingTopicReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
