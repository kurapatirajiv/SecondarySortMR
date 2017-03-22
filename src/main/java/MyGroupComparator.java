import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Rajiv on 3/22/17.
 *
 * Grouping based on Date of Birth only
 */
public class MyGroupComparator extends WritableComparator {

    public MyGroupComparator() {
        super(CustomWritable.class,true);
    }

    @Override
    public int compare(WritableComparable obj1, WritableComparable obj2) {

        CustomWritable key1 = (CustomWritable) obj1;
        CustomWritable key2 = (CustomWritable) obj2;

        return (key1.getDOB().compareTo(key2.getDOB()));


    }



}
