package Step2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Key2 implements WritableComparable<Key2>{
    private final Text lexama;

    public Key2(String lexama){
        this.lexama = new Text(lexama);
    }

    // Hadoop requires empty constructor
    public Key2(){
        this.lexama = new Text();
    }

    public String getLexama(){
        return this.lexama.toString();
    }

    @Override
    public String toString(){
        return lexama.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        lexama.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lexama.readFields(in);
    }

    @Override
    public int compareTo(Key2 o) {
        return this.lexama.compareTo(o.lexama);
    }

    @Override
    public int hashCode() {
        return this.lexama.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Key2) {
            Key2 other = (Key2) obj;
            return this.lexama.equals(other.lexama);
        }
        return false;
    }


}