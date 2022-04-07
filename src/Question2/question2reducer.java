package Question2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/** Reducer class for Total units_sold by year for a given country and a given item type
 * this is acheived by summing up the total units sold for each block produced by the mapper
 */
public class question2reducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    public void reduce (final Text key, final Iterable<DoubleWritable> values, final Context context)
        throws IOException,InterruptedException{
        Double sum = 0.0;
        // Calculate total units sold
        for (DoubleWritable value : values){
            sum += value.get();
        }
        // Create the <key,value output from the question1reducer>
        context.write(key, new DoubleWritable(sum));
    }
}
