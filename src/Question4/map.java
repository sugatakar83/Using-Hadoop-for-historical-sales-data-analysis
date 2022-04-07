package Question4;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/** for finding the top 10 orders over the years based on total profit,
 * we created tree maps for total profit and placed the data associated with it in the tree
 * the idea is to use Mappers to find local top 10 records, as there can be many Mappers running parallely on different blocks of data of a file.
 */
public class map extends Mapper<Object, Text, NullWritable, Text> {
    private TreeMap<Double, Text> orderdata = new TreeMap<Double, Text>();
    @Override
    public void map(Object key, Text values, Context context) {
        String data = values.toString();
        String[] field = data.split(",");
        Double totalprofit = null;
        if (null != field && field.length == 15 && field[14].length() > 0)
            totalprofit = Double.parseDouble(field[14]);
            Text val = new Text(values);
            orderdata.put(totalprofit, val);
            if (orderdata.size() > 10) {
                orderdata.remove(orderdata.firstKey());
            }
        }

    @Override
    /**
     * The important point to note here is that we use “context.write()” in cleanup() method
     * which runs only once at the end in the lifetime of Mapper. Mapper processes one key-value pair at a time
     * and writes them as intermediate output on local disk. But we have to process whole block (all key-value pairs)
     * to find top10, before writing the output, hence we use context.write() in cleanup().
     */
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double, Text> entry : orderdata.entrySet()) {
            context.write(NullWritable.get(), entry.getValue());
        }
    }
}

