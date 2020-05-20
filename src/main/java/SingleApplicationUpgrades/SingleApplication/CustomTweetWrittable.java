package SingleApplicationUpgrades.SingleApplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomTweetWrittable implements Writable {
    private IntWritable counter;
    private Text tweet;

    public CustomTweetWrittable() {
        this.counter = new IntWritable();
        this.tweet = new Text();
    }

    public CustomTweetWrittable(IntWritable counter, Text tweet) {
        this.counter = new IntWritable();
        this.tweet = new Text();
    }

    public IntWritable getCounter() {
        return counter;
    }

    public Text getTweet() {
        return tweet;
    }

    public void setCounter(IntWritable counter) {
        this.counter = counter;
    }

    public void setTweet(Text tweet) {
        this.tweet = tweet;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        counter.write(dataOutput);
        tweet.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        counter.readFields(dataInput);
        tweet.readFields(dataInput);
    }
}
