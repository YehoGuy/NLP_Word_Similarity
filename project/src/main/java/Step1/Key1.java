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

    // Hadoop requires
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
    public boolean equals(Object obj){
        if(obj instanceof Key1){
            Key1 other = (Key1) obj;
            return this.root==other.root && this.dependant==other.dependant && this.label==other.label;
        }
        return false;
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
        return this.root.toString().hashCode();
    }

    @Override
    /*
    * lexicographical by order root-->dependant-->label
    * '*' first
    */
    public int compareTo(Key1 other) {
        int cmp = 0;

        // root comparison
        if(this.root.equals(Consts.TEXT_STAR))
            cmp = -1;
        else if(other.root.equals(Consts.TEXT_STAR))
            cmp = 1;
        else
            this.root.compareTo(other.root);
        if(cmp!=0)
            return cmp;

        // dependant comparison
        if(this.dependant.equals(Consts.TEXT_STAR))
            cmp = -1;
        else if(other.dependant.equals(Consts.TEXT_STAR))
            cmp = 1;
        else
            this.dependant.compareTo(other.dependant);
        if(cmp!=0)
            return cmp;

        // label comparison;
        return this.label.compareTo(other.label);
    }



}
