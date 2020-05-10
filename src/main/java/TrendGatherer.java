import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrendGatherer {

    public static class CleanUpMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
        final static Pattern PATTERN = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            Matcher patternMatcher = PATTERN.matcher(value.toString());
            while(patternMatcher.find()) {
                String occurrence = patternMatcher.group();
                word.set(occurrence);
                context.write(word, one);
            }
        }
    }

    public static class CounterReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        private final LongWritable result = new LongWritable();
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String PATTERN = "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trending Gatherer");
        job.setJarByClass(TrendGatherer.class);

        // Topic Mapper
        Configuration topicMapperConf = new Configuration(false);
        topicMapperConf.set(RegexMapper.PATTERN, PATTERN);
        ChainMapper.addMapper(job, RegexMapper.class, Object.class, Text.class, Text.class,
                LongWritable.class, topicMapperConf);
        job.setReducerClass(CounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
