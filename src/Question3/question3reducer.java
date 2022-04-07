package Question3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.PriorityQueue;

/** max and min units_sold in any order for each year by country for a given item type
* The reducer iterates through the values to find the minimum and maximum units sold, and sums the counts.
* We start by initializing the output result for each input group.
* For each value in this group, if the output result’s minimum is not yet set, or the values minimum is less than results current minimum,
* we set the results minimum to the input value. The same logic applies to the maximum, except using a greater than operator.
* Each value’s count is added to a running sum.
* After determining the minimum and maximum salary from all input values, the final count is set to our output value. */
public class question3reducer extends Reducer<Text, CustomMinMaxTuple, Text, CustomMinMaxTuple> {
    // call the custom partitioner class to get the minimum and maximum values
    private CustomMinMaxTuple result = new CustomMinMaxTuple();
    public void reduce(Text key, Iterable<CustomMinMaxTuple> values, Context context)
            throws IOException, InterruptedException {
        result.setMin(null);
        result.setMax(null);

        long sum = 0;
        for (CustomMinMaxTuple minMaxCountTuple : values) {
            if (result.getMax() == null || (minMaxCountTuple.getMax() > result.getMax())) {
                result.setMax(minMaxCountTuple.getMax());
            }
            if (result.getMin() == null || (minMaxCountTuple.getMin() < result.getMin())) {
                result.setMin(minMaxCountTuple.getMin());
            }

        }
        // Write the reducer output to the context
        context.write(new Text(key.toString()), result);
    }
}
