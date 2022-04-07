package Question4_1;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Because we configured our job to have one reducer using job.setNumReduceTasks(1)
 * there will be one input group for this reducer that contains all the potential top ten records.
 * The reducer iterates through all these records and stores them in a TreeMap.
 * If the TreeMap’s size is above ten, the first element (lowest value) is remove from the map.
 * After all the values have been iterated over, the values contained in the TreeMap are flushed to the file system in descending order.
 * This ordering is achieved by getting the descending map from the TreeMap prior to outputting the values.
 * This can be done directly in the reduce method, because there will be only one input group, but doing it in the cleanup method would also work.
 */

public class reducer extends Reducer<Text,
        LongWritable, Text, LongWritable> {

    private TreeMap<Long, String> tmap2;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException
    {
        tmap2 = new TreeMap<Long, String>();
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException
    {

        // input data from mapper
        // key                values
        // name         [ count ]
        String name = key.toString();
        long count = 0;

        for (LongWritable val : values)
        {
            count = val.get();
        }

        // insert data into treeMap,
        // so we pass count as key
        tmap2.put(count, name);

        // we remove the first key-value
        // if it's size increases 10
        if (tmap2.size() > 10)
        {
            tmap2.remove(tmap2.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException
    {

        for (Map.Entry<Long, String> entry : tmap2.entrySet())
        {

            long count = entry.getKey();
            String name = entry.getValue();
            context.write(new Text(name),new LongWritable(count));
        }
    }
}