package Question1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/** Reducer class to find Average unit_price by country for a given item type in a certain year
 * This is done by calculating the sum of unit price and dividing it by number of occurances for that particular year*/
public class question1reducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    //Reduce the set of intermediate values which share a key to a smaller set of values.
    public void reduce (final Text key, final Iterable<DoubleWritable> values, final Context context)
            throws IOException,InterruptedException{
        Double sum = 0.0;
        Integer count = 0;
        for (DoubleWritable value : values){
            sum += value.get();
            count ++;
        }
        // Calculate average unit price
        Double ratio = sum/count;
        // Create the <key,value> question1reducer output
        context.write(key, new DoubleWritable(ratio));
    }
}

