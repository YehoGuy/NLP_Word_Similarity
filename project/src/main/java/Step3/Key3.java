package Step3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Key3 implements WritableComparable<Key3> {

    private final Text lexama1;
    private final Text lexama2;

    public Key3(String lexama1, String lexama2){
        if(lexama1.compareTo(lexama2) < 0){
            this.lexama1 = new Text(lexama1);
            this.lexama2 = new Text(lexama2);
        }else{
            this.lexama1 = new Text(lexama2);
            this.lexama2 = new Text(lexama1);
        }
    }

    // Hadoop requires empty constructor
    public Key3(){
        this.lexama1 = new Text();
        this.lexama2 = new Text();
    }

    public Text getLexama1(){
        return this.lexama1;
    }

    public Text getLexama2(){
        return this.lexama2;
    }

    @Override
    public String toString(){
        return lexama1.toString().trim()+"\t"+lexama2.toString().trim();
    }

    @Override
    public int hashCode() {
        return this.lexama1.toString().hashCode() + this.lexama2.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Key3) {
            Key3 other = (Key3) obj;
            return (this.lexama1.equals(other.lexama1) && this.lexama2.equals(other.lexama2)) ||
                    (this.lexama1.equals(other.lexama2) && this.lexama2.equals(other.lexama1));
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        lexama1.write(out);
        lexama2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lexama1.readFields(in);
        lexama2.readFields(in);
    }

    @Override
    public int compareTo(Key3 o) {
        if(this.lexama1.compareTo(o.lexama1) == 0)
            return this.lexama2.compareTo(o.lexama2);
        return this.lexama1.compareTo(o.lexama1);
    }





    
}
