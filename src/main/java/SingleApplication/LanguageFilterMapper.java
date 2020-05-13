package SingleApplication;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class LanguageFilterMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
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