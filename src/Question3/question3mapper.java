package Question3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/** max and min units_sold in any order for each year by country for a given item type
* Units sold information is stored in the 9th index so we are fetching the units sold and storing it in outTuple.
* The total units sold is output twice so that we can take advantage of the combiner optimization */
public class question3mapper extends Mapper<Object, Text, Text, CustomMinMaxTuple> {
    private CustomMinMaxTuple outTuple = new CustomMinMaxTuple();
    private Text keyfield = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] field = data.split(",");
        String country = field[2];
        String itemtype = field[3];
        String orderdate = field[6].substring(0,4);
        double unitssold = 0;
        if (null != field && field.length == 15 && field[9].length() >0) {
            unitssold=Double.parseDouble(field[9]);
            outTuple.setMin(unitssold);
            outTuple.setMax(unitssold);

           keyfield.set(country+","+itemtype+","+orderdate+",");

            context.write(keyfield, outTuple);
        }
    }
}
