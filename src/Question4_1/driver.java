package Question4_1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** instantiate a job which will run map reduce
 * It allows the user to configure the job, submit it, control its execution, and query the state.
 * The set methods only work until the job is submitted, afterwards they will throw an IllegalStateException. */
public class driver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception{
        // instantiate a job which will run map reduce
        Job job = Job.getInstance(getConf());
        job.setJobName("top_orders");
        job.setJarByClass(Question4_1.driver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setNumReduceTasks(1);
        Path inputfilepath = new Path("/Users/sugatakumarkar/Desktop/BITS/BDS/geosales.csv");
        Path outputfilepath = new Path("/Users/sugatakumarkar/Desktop/BITS/BDS/BDS-Assignment/question4_1_output");
        FileInputFormat.addInputPath(job,inputfilepath);
        FileOutputFormat.setOutputPath(job,outputfilepath);
        return job.waitForCompletion(true)  ? 0 : 1;

    }
    /** ToolRunner can be used to run classes implementing Tool interface.
     * It works in conjunction with GenericOptionsParser to parse the generic hadoop command line arguments
     * and modifies the Configuration of the Tool. The application-specific options are passed along without being modified. */
    public static void main(String[] args) throws Exception{
        int exitcode = ToolRunner.run(new Question4_1.driver(),args);
        System.exit(exitcode);
    }
}

