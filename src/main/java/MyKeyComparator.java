import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Rajiv on 3/22/17.
 *
 * Sort based on DOB and Name
 */
public class MyKeyComparator extends WritableComparator{

    public MyKeyComparator() {
        super(CustomWritable.class,true);
    }

    @Override
    public int compare(WritableComparable obj1, WritableComparable obj2) {
        CustomWritable key1 = (CustomWritable) obj1;
        CustomWritable key2 = (CustomWritable) obj2;
        int result = key1.getDOB().compareTo(key2.getDOB());
        if (result != 0)
        {
            return result;
        }
        return key1.getName().compareTo(key2.getName());
    }



}
