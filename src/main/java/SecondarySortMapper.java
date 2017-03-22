import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Rajiv on 3/22/17.
 *
 * To read the input file and write the DOB,Name pair as key and NullWritable as value
 */
public class SecondarySortMapper extends Mapper<LongWritable,Text,CustomWritable,NullWritable> {

    public void map(LongWritable key, Text values , Context context) throws IOException,InterruptedException{

        String val[] = values.toString().split(" ");
        CustomWritable obj = new CustomWritable(val[0],val[1]);
        context.write(obj,NullWritable.get());

    }



}
