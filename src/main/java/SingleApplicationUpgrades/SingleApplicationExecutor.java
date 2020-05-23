package SingleApplicationUpgrades;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static java.lang.Thread.sleep;

public class SingleApplicationExecutor extends Configured implements Tool {

    public int run (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // JOBS EXECUTIONS
        JobControl jobControl = new JobControl("Double Job");

        // JOB Map-Reduce HashtagCounter
        Configuration confJobTopicCounterCleanUp = new Configuration();
        Job jobTopicCounterCleanUp = Job.getInstance(confJobTopicCounterCleanUp, "HashtagCounter");
        jobTopicCounterCleanUp.setJarByClass(TopNPattern.class);

        FileInputFormat.addInputPath(jobTopicCounterCleanUp, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobTopicCounterCleanUp, new Path("temp"));
        confJobTopicCounterCleanUp.set("lang", args[3]);

        // LowerCaseMapper
        ChainMapper.addMapper(jobTopicCounterCleanUp, CleanUp.LowerCaseMapper.class, Object.class,
                Text.class, IntWritable.class, CustomTweetWritable.class, confJobTopicCounterCleanUp);

        // RemovalLackingFieldsMapper
        ChainMapper.addMapper(jobTopicCounterCleanUp, CleanUp.RemoveLackingFieldsMapper.class, IntWritable.class,
                CustomTweetWritable.class, IntWritable.class, CustomTweetWritable.class, confJobTopicCounterCleanUp);

        // RemovalNotUsedFieldsMapper
        ChainMapper.addMapper(jobTopicCounterCleanUp, CleanUp.FieldSelectorMapper.class, IntWritable.class,
                CustomTweetWritable.class, IntWritable.class, CustomTweetWritable.class, confJobTopicCounterCleanUp);

        // FilterLanguageMapper
        ChainMapper.addMapper(jobTopicCounterCleanUp, CleanUp.LanguageFilterMapper.class, IntWritable.class,
                CustomTweetWritable.class, IntWritable.class, CustomTweetWritable.class, confJobTopicCounterCleanUp);

        // TrendingTopicMapper
        ChainMapper.addMapper(jobTopicCounterCleanUp, TrendingTopics.TrendingTopicMapper.class, IntWritable.class,
                CustomTweetWritable.class, Text.class, IntWritable.class, confJobTopicCounterCleanUp);
        jobTopicCounterCleanUp.setCombinerClass(TrendingTopics.TrendingTopicReducer.class);

        // TrendingTopicReducer
        jobTopicCounterCleanUp.setReducerClass(TrendingTopics.TrendingTopicReducer.class);

        jobTopicCounterCleanUp.setOutputKeyClass(Text.class);
        jobTopicCounterCleanUp.setOutputValueClass(LongWritable.class);

        ControlledJob controlledJobTopicCounter = new ControlledJob(confJobTopicCounterCleanUp);
        controlledJobTopicCounter.setJob(jobTopicCounterCleanUp);
        jobControl.addJob(controlledJobTopicCounter);

        // JOB Top-N
        Configuration confJobTopN = new Configuration();
        confJobTopN.set("N", args[2]);

        Job jobTopN = Job.getInstance(confJobTopN, "TopN");
        jobTopN.setJarByClass(TopNPattern.class);

        FileInputFormat.addInputPath(jobTopN, new Path("temp"));
        FileOutputFormat.setOutputPath(jobTopN, new Path(args[1]));

        jobTopN.setMapperClass(TopNPattern.TopNMapper.class);
        jobTopN.setReducerClass(TopNPattern.TopNReducer.class);

        jobTopN.setMapOutputKeyClass(NullWritable.class);
        jobTopN.setMapOutputValueClass(CustomTweetWritable.class);

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
        int exitCode = ToolRunner.run(new SingleApplicationExecutor(), args);
        System.exit(exitCode);
    }
}
