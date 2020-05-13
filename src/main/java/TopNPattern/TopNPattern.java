package TopNPattern;

import SingleApplication.LowerCaseMapper;
import SingleApplication.TopicGathererMapper;
import TrendingTopics.TrendingTopics;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.TreeMap;

public class TopNPattern {
    public static class TopicMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                System.out.println("TopicMapper: " + value.toString());
                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(value.toString());
                JSONObject entitiesJSONObj = (JSONObject) jsonObject.get("entities");
                JSONArray hashtagsArray = (JSONArray) entitiesJSONObj.get("hashtags");

                for(Object jsonArrayElem: hashtagsArray) {
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

    public static class CounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable result = new LongWritable();
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("CounterReducer: " + key.toString());
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TopNMapper extends Mapper<Text, LongWritable, NullWritable, Text> {
        private final TreeMap<LongWritable, Text> topNTreeMap = new TreeMap<>();

        public void map(Text key, LongWritable value, Context context) {
            System.out.println("TOPNMAPPER: " + key.toString());
            int N = 2 * Integer.parseInt(context.getConfiguration().get("N"));
            Text word = new Text();
            word.set(key.toString() + "," + value);
            topNTreeMap.put(value, word);
            if (topNTreeMap.size() > N) {
                topNTreeMap.remove(topNTreeMap.firstKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Text t : topNTreeMap.values()) {
                System.out.println("cleanup: " + t.toString());
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private final TreeMap<LongWritable, Text> topNTreeMap = new TreeMap<>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int N = Integer.parseInt(context.getConfiguration().get("N"));
            for(Text value: values) {
                String[] valueKey = value.toString().split(",");
                LongWritable weight = new LongWritable(Integer.parseInt(valueKey[1]));
                Text topic = new Text(valueKey[0]);
                topNTreeMap.put(weight, topic);

                if (topNTreeMap.size() > N) {
                    topNTreeMap.remove(topNTreeMap.firstKey());
                }

                for (Text word : topNTreeMap.descendingMap().values()) {
                    context.write(NullWritable.get(), word);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "TOP N PATTERN");
        job.setJarByClass(TopNPattern.class);

        // TopicMapper
        Configuration topicMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, TopicMapper.class, Object.class, Text.class, Text.class,
                LongWritable.class, topicMapperConf);

        // TopicReducer
        Configuration counterReducerConf = new Configuration(false);
        ChainReducer.setReducer(job, CounterReducer.class, Text.class, LongWritable.class, Text.class,
                LongWritable.class, counterReducerConf);

        // TOP - N
        Configuration topNComputerConf = new Configuration(false);
        topNComputerConf.set("N", args[2]);
        ChainReducer.addMapper(job, TopNMapper.class, Text.class, LongWritable.class, NullWritable.class,
                Text.class, topNComputerConf);

        ChainReducer.setReducer(job, TopNReducer.class, NullWritable.class, Text.class, NullWritable.class,
                Text.class, topNComputerConf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
