package TextCleanUp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class TextCleanUp {

    public static class LowerCaseMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException { ;
            word.set(value.toString().toLowerCase());
            context.write(word, one);
        }
    }

    public static class RemovalLackingFieldsMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
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

    public static class RemovalNotUsedFieldsMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
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

    public static class filterLanguageMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
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


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "Text Clean Up");
        job.setJarByClass(TextCleanUp.class);

        // LowerCaseMapper
        Configuration lowerCaseMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, LowerCaseMapper.class, Object.class, Text.class, Text.class,
                LongWritable.class, lowerCaseMapperConf);

        // RemovalLackingFieldsMapper
        Configuration removalLackingFieldsMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, RemovalLackingFieldsMapper.class, Text.class, LongWritable.class, Text.class,
                LongWritable.class, removalLackingFieldsMapperConf);

        // RemovalNotUsedFieldsMapper
        Configuration removalNotUsedFieldsMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, RemovalNotUsedFieldsMapper.class, Text.class, LongWritable.class, Text.class,
                LongWritable.class, removalNotUsedFieldsMapperConf);

        // filterLanguageMapper
        Configuration filterLanguageMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, filterLanguageMapper.class, Text.class, LongWritable.class, Text.class,
                LongWritable.class, filterLanguageMapperConf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
