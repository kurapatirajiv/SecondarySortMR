import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Rajiv on 3/22/17.
 */
public class SecondarySortReducer extends Reducer<CustomWritable, NullWritable, CustomWritable, NullWritable> {

    public void reduce(CustomWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }

    }
}
