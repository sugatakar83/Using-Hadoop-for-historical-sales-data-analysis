package Question4_1;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Each mapper determines the top ten records of its input split and outputs them to the reduce phase.
 * The mappers are essentially filtering their input split to the top ten records, and the reducer is responsible for the final ten.
 * The mapper processes all input records and stores them in a Tree Map.
 * If there are more than ten records in our Tree Map, the first element can be removed.After all the records have been processed,
 * the top ten records in the Tree Map are output to the reducers in the cleanup method.
 * This method gets called once after all key/value pairs have been through map, just like how setup is called once before any calls to map.
 */
public class mapper extends Mapper<Object,
        Text, Text, LongWritable> {

private TreeMap<Long, String> tmap;

@Override
public void setup(Context context) throws IOException,
        InterruptedException
        {
        tmap = new TreeMap<Long, String>();
        }

@Override
public void map(Object key, Text value,
        Context context) throws IOException,
        InterruptedException
        {
        // input data format => orderid
        // totalprofit  (tab seperated)
        // we split the input data
        String[] tokens = value.toString().split(",");
        String orderid = tokens[6].substring(0,4)+","+tokens[7];
        long totalprofit = (long)Double.parseDouble(tokens[14]);

        // insert data into treeMap,
        // so we pass totalprofit as key
        tmap.put(totalprofit, orderid);

        // we remove the first key-value
        // if it's size increases 10
        if (tmap.size() > 10)
        {
        tmap.remove(tmap.firstKey());
        }
        }

@Override
public void cleanup(Context context) throws IOException,
        InterruptedException
        {
        for (Map.Entry<Long, String> entry : tmap.entrySet())
        {

        long count = entry.getKey();
        String name = entry.getValue();

        context.write(new Text(name), new LongWritable(count));
        }
        }
        }