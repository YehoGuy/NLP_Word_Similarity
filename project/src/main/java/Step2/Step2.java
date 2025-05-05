package Step2;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import Helpers.Consts;
import Helpers.S3Methods;
import Step1.Key1;

public class Step2 {

    public static class Mapper2 extends Mapper<Key1, LongWritable, Key2, Value2> {
        
        private long countL=-1;
        private long countF=-1;
        private HashMap<String,Long> Lel = null;
        private long sumLel = 0;
        private Key1 currentFef = null;
        private long currentFefCount = 1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // retrieve countL - compute countF
            countL = S3Methods.retrieveCountL();
            countF = countL*Consts.NUMBER_OF_DEPENDENCY_LABELS;
            // retrieve CountLel
            Lel = S3Methods.retrieveLel();
            // compute sumLel
            for(Long value : Lel.values())
                sumLel+=value;
            // init currentFef
            currentFef = new Key1();
            currentFefCount = 1;
        }

        @Override
        public void map(Key1 key, LongWritable value, Context context) throws IOException, InterruptedException {
            if(key.getRoot().equals(Consts.STAR)){
                currentFef = key;
                currentFefCount = value.get();
            } else{
                try{
                    Key2 newKey = new Key2(key.getRoot());
                    Value2 newValue = new Value2(key.getDependant(),key.getLabel(),computeAssocFreq(value.get()),computeAssocProb(key,value.get()),computeAssocPMI(key,value.get()),computeAssocTTest(key,value.get()));
                    context.write(newKey,newValue);
                }catch(Exception e){
                    Key2 newKey = new Key2("[Map2Error] "+key.getRoot()+" "+key.getDependant()+"-"+key.getLabel());
                    Value2 newValue = new Value2();
                    context.write(newKey,newValue);
                }
            }
        }

        public double computeAssocFreq(long value){
            // count(l,f)
            return value;
        }

        public double computeAssocProb(Key1 key, long value){
            // P(F=f|L=l) = count(l,f)/count(L=l)
            double ans = ((double)value)/Lel.get(key.getRoot());
            return ans;
        }

        public double computeAssocPMI(Key1 key, long value){
            double plf = (double)(value) / countL; // P(L=l,F=f)
            double pl = (double)(Lel.get(key.getRoot())) / sumLel; // P(L=l)
            double pf = (double)(currentFefCount) / countF; // g(P(F=f))
            double x = plf / (pl*pf);
            return Math.log(x)/Math.log(2);
        }

        public double computeAssocTTest(Key1 key, long value){
            double plf = (double)(value) / countL; // P(L=l,F=f)
            double pl = (double)(Lel.get(key.getRoot())) / sumLel; // P(L=l)
            double pf = (double)(currentFefCount) / countF; // g(P(F=f))
            return (plf-pl*pf)/Math.sqrt(pl*pf);
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }

    }
    

    public static class Partitioner2 extends Partitioner<Key2, Value2> {
        @Override
        public int getPartition(Key2 key, Value2 value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }



    
    public static class Reducer2 extends Reducer<Key2,Value2,Key2,Map2> {
        
        @Override
        public void reduce(Key2 key, Iterable<Value2> values, Context context) throws IOException,  InterruptedException {
            MapWritable map = new MapWritable();
            for(Value2 value : values)
                map.put(new Key2(value.getFeature()), new Value2(value.getDependant(),value.getLabel(),value.getFreq(),value.getProb(),value.getPmi(),value.getTtest()));
            context.write(key, new Map2(map));
        }

        

    }
    
    




    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2: Compute Vectors");
        job.setJarByClass(Step2.class);
        job.setMapperClass(Mapper2.class);
        job.setPartitionerClass(Partitioner2.class);
        job.setReducerClass(Reducer2.class);
        job.setMapOutputKeyClass(Key2.class);
        job.setMapOutputValueClass(Value2.class);
        job.setOutputKeyClass(Key2.class);
        job.setOutputValueClass(Map2.class);

        //for running on AWS Emr:
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(Consts.STEP2_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(Consts.STEP2_OUTPUT));

        //for testing locally:
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        //FileInputFormat.addInputPath(job, new Path("test.txt"));
        //TextOutputFormat.setOutputPath(job, new Path("output_test"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    

}