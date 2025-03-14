package Step2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Helpers.Consts;
import Helpers.S3Methods;
import Step1.Key1;

public class Step2 {

    public static class Mapper2 extends Mapper<Key1, LongWritable, Key1, LongWritable> {
        
        private long countL=-1;
        private long countF=-1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // retrieve countL - compute countF
            countL = S3Methods.retrieveCountL();
            countF = countL*Consts.NUMBER_OF_DEPENDENCY_LABELS;
        }

        @Override
        public void map(Key1 key, LongWritable value, Context context) throws IOException, InterruptedException {
            
        
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            

            super.cleanup(context);
        }

    }
    

    public static class Partitioner2 extends Partitioner<Key1, LongWritable> {
        @Override
        public int getPartition(Key1 key, LongWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }



    
    public static class Reducer2 extends Reducer<Key1,LongWritable,Key1,LongWritable> {
        
        @Override
        public void reduce(Key1 key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            
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
        job.setMapOutputKeyClass(Key1.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Key1.class);
        job.setOutputValueClass(LongWritable.class);

        //for running on AWS Emr:
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
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