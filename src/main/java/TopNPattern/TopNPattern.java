package TopNPattern;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.*;

import static java.lang.Thread.sleep;

public class TopNPattern extends Configured implements Tool {
    public static class TopicMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
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
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TopNMapper extends Mapper<Text, Text, NullWritable, Text> {
        private final MultiValuedMap<Integer, Text> multiMap = new ArrayListValuedHashMap<>();

        public void map(Text key, Text value, Context context) {
            int N = 2 * Integer.parseInt(context.getConfiguration().get("N"));
            Text word = new Text();
            int weight = Integer.parseInt(value.toString());
            word.set(key.toString() + "," + value.toString());

            multiMap.put(weight, word);
            if (multiMap.size() > N) {
                int minKey = getMinKey(multiMap.keySet());
                Text val = multiMap.get(minKey).iterator().next();
                multiMap.removeMapping(minKey, val);
            }
        }

        private int getMinKey(Set<Integer> keys) {
            Iterator<Integer> keysIter = keys.iterator();
            int minVal = keysIter.next();
            while(keysIter.hasNext()) {
                int val = keysIter.next();
                minVal = Math.min(val, minVal);
            }
            return minVal;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Text t : multiMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private final MultiValuedMap<Integer, Text> multiMap = new ArrayListValuedHashMap<>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int N = Integer.parseInt(context.getConfiguration().get("N"));
            for(Text value: values) {
                String[] valueKey = value.toString().split(",");
                int weight = Integer.parseInt(valueKey[1]);
                Text topic = new Text(valueKey[0]);
                multiMap.put(weight, topic);
            }
            List<Integer> sortedKeys = sort(multiMap.keySet());
            int elemsWritten = 0;

            for(int k: sortedKeys) {
                for(Text t: multiMap.get(k)) {
                    context.write(NullWritable.get(), t);
                    elemsWritten++;
                    if(elemsWritten == N) { break; }
                }
                if(elemsWritten == N) { break; }
            }
        }

        private List<Integer> sort(Set<Integer> keys) {
            List<Integer> sortedKeys = new ArrayList<>(keys);
            sortedKeys.sort(Integer::compareTo);
            Collections.reverse(sortedKeys);
            return sortedKeys;
        }
    }


    public int run (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // JOBS EXECUTIONS
        JobControl jobControl = new JobControl("Double Job");

        // JOB Map-Reduce HashtagCounter
        Configuration confJobTopicCounter = new Configuration();
        Job jobTopicCounter = Job.getInstance(confJobTopicCounter, "HashtagCounter");
        jobTopicCounter.setJarByClass(TopNPattern.class);

        FileInputFormat.addInputPath(jobTopicCounter, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobTopicCounter, new Path("src/main/resources/temp"));

        jobTopicCounter.setMapperClass(TopicMapper.class);
        jobTopicCounter.setReducerClass(CounterReducer.class);

        jobTopicCounter.setOutputKeyClass(Text.class);
        jobTopicCounter.setOutputValueClass(LongWritable.class);

        ControlledJob controlledJobTopicCounter = new ControlledJob(confJobTopicCounter);
        controlledJobTopicCounter.setJob(jobTopicCounter);
        jobControl.addJob(controlledJobTopicCounter);

        // JOB Top-N
        Configuration confJobTopN = new Configuration();
        confJobTopN.set("N", args[2]);

        Job jobTopN = Job.getInstance(confJobTopN, "TopN");
        jobTopN.setJarByClass(TopNPattern.class);

        FileInputFormat.addInputPath(jobTopN, new Path("src/main/resources/temp"));
        FileOutputFormat.setOutputPath(jobTopN, new Path(args[1]));

        jobTopN.setMapperClass(TopNMapper.class);
        jobTopN.setReducerClass(TopNReducer.class);

        jobTopN.setOutputKeyClass(NullWritable.class);
        jobTopN.setOutputValueClass(Text.class);
        jobTopN.setInputFormatClass(KeyValueTextInputFormat.class);

        ControlledJob controlledJobTopN = new ControlledJob(confJobTopN);
        controlledJobTopN.setJob(jobTopN);
        controlledJobTopN.addDependingJob(controlledJobTopicCounter);
        jobControl.addJob(controlledJobTopN);

        Thread runJControl = new Thread(jobControl);
        runJControl.start();

        while (!jobControl.allFinished()) {
            sleep(5000);
        }
        return jobTopN.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TopNPattern(), args);
        System.exit(exitCode);
    }

}
