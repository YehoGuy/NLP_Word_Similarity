package Step3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class Value3 implements Writable{
    DoubleWritable[] combinationScores;
    boolean isEmpty;

    public Value3(DoubleWritable[] combinationScores){
        this.combinationScores = new DoubleWritable[24];
        for(int i=0; i<24; i++){
            this.combinationScores[i] = combinationScores[i];
        }
        isEmpty = false;
    }

    public Value3(){
        this.combinationScores = new DoubleWritable[24];
        for(int i=0; i<24; i++){
            this.combinationScores[i] = new DoubleWritable(-1);
        }
        isEmpty = true;
    }

    @Override
    public String toString(){
        if(!isEmpty){
            String result = "";
            for(int i=0; i<23; i++){
                result += this.combinationScores[i].get()+" , ";
            }
            result += this.combinationScores[23].get();
            return "["+result+"]";
        }
        return "";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for(int i=0; i<24; i++){
            this.combinationScores[i].write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for(int i=0; i<24; i++){
            this.combinationScores[i].readFields(in);
        }
    }

}
