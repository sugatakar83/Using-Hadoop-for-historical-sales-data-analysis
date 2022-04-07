package Question1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/** Mapper class for Average unit_price by country for a given item type in a certain year
* The framework first calls setup(org.apache.hadoop.mapreduce.Mapper.Context), f
* ollowed by map(Object, Object, org.apache.hadoop.mapreduce.Mapper.Context)
* for each key/value pair in the InputSplit.
* Finally cleanup(org.apache.hadoop.mapreduce.Mapper.Context) is called. */
public class question1mapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
    @Override
    public void map(LongWritable key,Text value,Context context)
            throws IOException, InterruptedException{
        String line = value.toString();
        String data[] = line.split(",");
        try{
            // Reading the required information from the input file
            String country = data[2];
            String itemtype = data[3];
            String orderdate = data[6].substring(0,4);
            Double unitprice = Double.parseDouble(data[10]);
            // creating the <key,value> question1mapper output
            context.write(new Text(orderdate+","+country+","+itemtype+","),new DoubleWritable(unitprice));
        }catch (Exception e){}

    }
}

