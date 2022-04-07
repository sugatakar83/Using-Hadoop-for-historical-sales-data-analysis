package Question1;
// importing necessary packages required for the driver class
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// setting up the job to run map reduce
public class question1main extends Configured implements Tool {
    @Override
    /** instantiate a job which will run map reduce
     * It allows the user to configure the job, submit it, control its execution, and query the state.
     * The set methods only work until the job is submitted, afterwards they will throw an IllegalStateException.*/
    public int run(String[] args) throws Exception{

        Job job = Job.getInstance(getConf());
        job.setJobName("average_unit_price");
        //Set the Jar by finding where a given class came from.
        job.setJarByClass(question1main.class);
        //Set the key class for the job output data.
        job.setOutputKeyClass(Text.class);
        //Set the value class for job outputs.
        job.setOutputValueClass(DoubleWritable.class);
        //Set the Mapper for the job.
        job.setMapperClass(question1mapper.class);
        //Set the Reducer for the job.
        job.setReducerClass(question1reducer.class);
        //Read the input file and create the target directory where the question1reducer output will be stored
        Path inputfilepath = new Path("/Users/sugatakumarkar/Desktop/BITS/BDS/geosales.csv");
        Path outputfilepath = new Path("/Users/sugatakumarkar/Desktop/BITS/BDS/BDS-Assignment/question1_output");
        FileInputFormat.addInputPath(job,inputfilepath);
        FileOutputFormat.setOutputPath(job,outputfilepath);
        return job.waitForCompletion(true)  ? 0 : 1;

    }
    /** ToolRunner can be used to run classes implementing Tool interface.
    * It works in conjunction with GenericOptionsParser to parse the generic hadoop command line arguments
    * and modifies the Configuration of the Tool. The application-specific options are passed along without being modified. */
    public static void main(String[] args) throws Exception{

        int exitcode = ToolRunner.run(new question1main(),args);
        System.exit(exitcode);
    }
}
