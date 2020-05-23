package SingleApplicationUpgrades;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopNPattern {
    public static class TopNMapper extends Mapper<Text, Text, NullWritable, CustomTweetWritable> {
        private final MultiValuedMap<Integer, CustomTweetWritable> multiMap = new ArrayListValuedHashMap<>();

        public void map(Text key, Text value, Context context) {
            int N = 2 * Integer.parseInt(context.getConfiguration().get("N"));
            int weight = Integer.parseInt(value.toString());
            multiMap.put(weight, new CustomTweetWritable(new IntWritable(weight), new Text(key)));
            if (multiMap.size() > N) {
                int minKey = getMinKey(multiMap.keySet());
                CustomTweetWritable val = multiMap.get(minKey).iterator().next();
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
            for (CustomTweetWritable t : multiMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, CustomTweetWritable, NullWritable, Text> {
        private final MultiValuedMap<Integer, Text> multiMap = new ArrayListValuedHashMap<>();

        public void reduce(NullWritable key, Iterable<CustomTweetWritable> values, Context context) throws IOException, InterruptedException {
            int N = Integer.parseInt(context.getConfiguration().get("N"));
            for(CustomTweetWritable value: values) {
                int weight = value.getCounter().get();
                Text topic = new Text (value.getTweet());
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

}
