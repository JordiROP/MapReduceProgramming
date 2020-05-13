package SingleApplication;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LowerCaseMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final static LongWritable val = new LongWritable();
    private final Text json = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException { ;
        json.set(value.toString().toLowerCase());
        context.write(json, val);
    }
}
