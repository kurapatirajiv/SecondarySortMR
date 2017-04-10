import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Rajiv on 3/22/17.
 */
public class Driver {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2) {
            System.out.print("Not Enough arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);


        job.setJarByClass(Driver.class);
        job.setJobName("Secondary Sort Application");

        job.setMapperClass(SecondarySortMapper.class);

        // Define Mapper output classes
        job.setMapOutputKeyClass(CustomWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Setting Classes necessary for Performing Secondary Sort
        job.setPartitionerClass(MyPartitioner.class);
        job.setSortComparatorClass(MyKeyComparator.class);
        // Below is only needed if you performing an aggregation on the keys
        // job.setGroupingComparatorClass(MyGroupComparator.class);

        job.setReducerClass(SecondarySortReducer.class);

        // Specify key / value
        job.setOutputKeyClass(CustomWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
