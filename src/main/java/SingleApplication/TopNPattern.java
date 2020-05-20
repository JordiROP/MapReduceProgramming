package SingleApplication;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopNPattern {

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

}
