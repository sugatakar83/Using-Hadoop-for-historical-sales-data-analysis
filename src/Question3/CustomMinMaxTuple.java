package Question3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;



/** The CustomMinMaxTuple is a Writable object that stores three values.
* This class is used as the output value from the mapper. While these values can be crammed into a Text object with some delimiter,
* it is typically a better practice to create a custom Writable.
* Method to return the min value
* The minimum and maximum units sold per item can be calculated for each local map task without having an effect on the final minimum and maximum.
* The counting operation is an associative and commutative operation and won’t be harmed by using a combiner.
 */
public class CustomMinMaxTuple implements Writable {
    private Double min = new Double(0);
    private Double max = new Double(0);
    private long count = 1;

    public Double getMin() {
        return min;
    }

    // Method to set the min value
    public void setMin(Double min) {
        this.min = min;
    }

    // Method to return the max value
    public Double getMax() {
        return max;
    }

    // Method to set the max value
    public void setMax(Double max) {
        this.max = max;
    }

    public void readFields(DataInput in) throws IOException {
        min = in.readDouble();
        max = in.readDouble();

    }
    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);

    }
    public String toString() {
        return min + "," + max;
    }
}