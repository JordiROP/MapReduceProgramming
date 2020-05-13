package SingleApplication;

import TextCleanUp.TextCleanUp;
import TrendingTopics.TrendingTopics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SingleApplicationExecutor {

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

        // RemoveLackingFieldsMapper
        Configuration removeLackingFieldsMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, RemoveLackingFieldsMapper.class, Text.class, LongWritable.class, Text.class,
                LongWritable.class, removeLackingFieldsMapperConf);

        // FieldSelectorMapper
        Configuration fieldSelectorMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, FieldSelectorMapper.class, Text.class, LongWritable.class, Text.class,
                LongWritable.class, fieldSelectorMapperConf);

        // LanguageFilterMapper
        Configuration languageFilterMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, LanguageFilterMapper.class, Text.class, LongWritable.class, Text.class,
                LongWritable.class, languageFilterMapperConf);

        // TopicGathererMapper
        Configuration topicGathererMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, TopicGathererMapper.class, Text.class, LongWritable.class, Text.class,
                LongWritable.class, topicGathererMapperConf);

        // Counter Reducer
        job.setReducerClass(TrendingTopics.CounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
