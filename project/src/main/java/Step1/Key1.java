package Step1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import Helpers.Consts;

/*
 * Each Key represents an edge int the parsed tree.
 * key's structure is as follows:
 *    root: Text       is the root word of the parsed tree.
 *    dependant: Text  is a related/depended word to root;
 *    label: Text      is the relation-label between the words.
 */
public class Key1 implements WritableComparable<Key1>{
    private final Text root;
    private final Text dependant;
    private final Text label;

    public Key1(String root, String dependant, String label){
        this.root = new Text(root);
        this.dependant = new Text(dependant);
        this.label = new Text(label);
    }

    // Hadoop requires empty constructor
    public Key1(){
        this.root = new Text();
        this.dependant = new Text();
        this.label = new Text();
    }

    public String getRoot(){
        return this.root.toString();
    }

    public String getDependant(){
        return this.dependant.toString();
    }

    public String getLabel(){
        return this.label.toString();
    }


    @Override
    public String toString(){
        return root+" "+dependant+"-"+label;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        root.write(out);
        dependant.write(out);
        label.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        root.readFields(in);
        dependant.readFields(in);
        label.readFields(in);
    }

    //so that all keys with the same first word will be sent to the same reducer
    @Override
    public int hashCode() {
        return this.dependant.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Key1) {
            Key1 key = (Key1) obj;
            return root.equals(key.root) && dependant.equals(key.dependant) && label.equals(key.label);
        }
        return false;
    }

    @Override
    /*
     * compareTo method compares the keys lexicographically-like
     * except that "*" is considered to be the smallest word
     */
    public int compareTo(Key1 other){
        if(this.root.equals(other.root) &&
           this.dependant.equals(other.dependant) &&
           this.label.equals(other.label))
            {return 0;}
        //  compare second word
        if(this.dependant.equals(Consts.TEXT_STAR))
            return -1;
        if(other.dependant.equals(Consts.TEXT_STAR))
            return 1;
        if(this.dependant.compareTo(other.dependant) < 0)
            return -1;
        if(this.dependant.compareTo(other.dependant) > 0)
            return 1;
        // compare third word
        if(this.label.equals(Consts.TEXT_STAR))
            return -1;
        if(other.label.equals(Consts.TEXT_STAR))
            return 1;
        if(this.label.compareTo(other.label) < 0)
            return -1;
        if(this.label.compareTo(other.label) > 0)
            return 1;
        // compare first word
        if(this.root.equals(Consts.TEXT_STAR))
            return -1;
        if(other.root.equals(Consts.TEXT_STAR))
            return 1;
        if(this.root.compareTo(other.root) < 0)
            return -1;
        if(this.root.compareTo(other.root) > 0)
            return 1;
        // if all are equal
        return 0;
    }



}
