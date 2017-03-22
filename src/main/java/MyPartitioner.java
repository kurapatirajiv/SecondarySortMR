import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Rajiv on 3/22/17.
 * Creating partition file needed for secondary sort
 */
public class MyPartitioner extends Partitioner<CustomWritable, NullWritable> {

    @Override
    public int getPartition(CustomWritable key, NullWritable value, int numReducerTasks) {

        return (key.getDOB().hashCode() % numReducerTasks);
    }

}
