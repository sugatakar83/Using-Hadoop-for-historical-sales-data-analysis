package Question4;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Reducer processes one key-value pair at a time and writes them as final output on HDFS.
 * But we have to process all key-value pairs to find top10, before writing the output, hence we use cleanup().
 */
public class reduce extends Reducer<NullWritable, Text, NullWritable, Text> {
    private TreeMap<Double, Text> orderdata = new TreeMap<Double, Text>();
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String data = value.toString();
            String[] field = data.split(",");
            if (field.length == 15) {
                orderdata.put(Double.parseDouble(field[7]), new Text(value));
                if (orderdata.size() > 10) {
                    orderdata.remove(orderdata.firstKey());
                }
            }
        }
        for (Text t : orderdata.descendingMap().values()) {
            context.write(NullWritable.get(), t);
        }
    }
}

