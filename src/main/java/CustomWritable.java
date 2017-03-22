import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Rajiv on 3/22/17.
 *
 * Custom Writable class for reading Date of Birth and Name
 */
public class CustomWritable implements WritableComparable<CustomWritable> {

    private String DOB;
    private String name;

    public CustomWritable() {
    }

    public CustomWritable(String DOB, String name) {
        this.DOB = DOB;
        this.name = name;
    }

    public String getDOB() {
        return DOB;
    }

    public void setDOB(String DOB) {
        this.DOB = DOB;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomWritable that = (CustomWritable) o;

        if (DOB != null ? !DOB.equals(that.DOB) : that.DOB != null) return false;
        return !(name != null ? !name.equals(that.name) : that.name != null);

    }

    @Override
    public int hashCode() {
        int result = DOB != null ? DOB.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CustomWritable{" +
                "DOB='" + DOB + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    public int compareTo(CustomWritable obj) {
        int result = DOB.compareTo(obj.DOB);
        if (result != 0) {
            return result;
        }
        return name.compareTo(obj.name);
    }

    public void write(DataOutput dataOutput) throws IOException {


        WritableUtils.writeString(dataOutput, DOB);
        WritableUtils.writeString(dataOutput, name);

    }

    public void readFields(DataInput dataInput) throws IOException {


        DOB = WritableUtils.readString(dataInput);
        name = WritableUtils.readString(dataInput);

    }


}
