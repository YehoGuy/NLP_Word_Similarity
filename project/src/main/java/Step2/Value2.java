package Step2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Value2 implements Writable{
    private final Text feature;
    private final DoubleWritable freq;
    private final DoubleWritable prob;
    private final DoubleWritable pmi;
    private final DoubleWritable ttest;

    public Value2(String dependant, String label, double freq, double prob, double pmi, double ttest){
        this.feature = new Text(dependant+"-"+label);
        this.freq = new DoubleWritable(freq);
        this.prob = new DoubleWritable(prob);
        this.pmi = new DoubleWritable(pmi);
        this.ttest = new DoubleWritable(ttest);
    }

    public Value2(){
        this.feature = new Text("null-null");
        this.freq = new DoubleWritable(-1);
        this.prob = new DoubleWritable(-1);
        this.pmi = new DoubleWritable(-1);
        this.ttest = new DoubleWritable(-1);
    }

    @Override
    public String toString(){
        return "("+feature+" , <"+freq+","+prob+","+pmi+","+ttest+">) ";
    }

    public double getAssociation(int associationIndex){
        switch(associationIndex%4){
            case 0:
                return this.freq.get();
            case 1:
                return this.prob.get();
            case 2:
                return this.pmi.get();
            case 3:
                return this.ttest.get();
            default:
                return -1;
        }
    }

    public String getFeature(){
        return this.feature.toString();
    }

    public String getDependant(){
        String[] parts = this.feature.toString().split("-");
        if(parts!=null && parts.length>0)
            return parts[0];
        return "nullvalue2dependant";
    }

    public String getLabel(){
        String[] parts = this.feature.toString().split("-");
        if(parts!=null && parts.length>1)
            return parts[1];
        return "nullvalue2label";
    }

    public double getFreq(){
        return this.freq.get();
    }

    public double getProb(){
        return this.prob.get();
    }

    public double getPmi(){
        return this.pmi.get();
    }

    public double getTtest(){
        return this.ttest.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        feature.write(out);
        freq.write(out);
        prob.write(out);
        pmi.write(out);
        ttest.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        feature.readFields(in);
        freq.readFields(in);
        prob.readFields(in);
        pmi.readFields(in);
        ttest.readFields(in);
    }

    
}
