package Step1;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import Helpers.Consts;
import Helpers.S3Methods;

public class Step1 {

    public static class Mapper1 extends Mapper<LongWritable, Text, Key1, LongWritable> {
        // relevantWords is a datastructure (set) containing only the relevant target words,
        // for optimization - minimizing number of keys outputted from the Mapper.
        private final HashSet<String> relevantWords = new HashSet<>();

        private long countL=0;
        private String latestRoot = "";

        private Key1 wKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // retrieve relevant words file and setup the relevantWords Set.
            AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
			S3Object s3Object = s3.getObject(new GetObjectRequest(Consts.BUCKET, Consts.RELEVANT_WORDS_S3_KEY));
			BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
            String line;
			while ((line = reader.readLine()) != null) 
                relevantWords.add(line.toLowerCase());
        }

        /*
         * isLegalWord is used to filter bad (jibberish) words.
         * e.g numbers, special characters, etc.
         * in order to compute that quickly O(1), 
         * we assume that if the first character is a letter, 
         * and that the last character is a letter,
         * then the rest of the characters are also letters.
         * (from observation, the bad words are either numbers, or words that start/end with special characters)
         */
        public boolean isLegalWord(String word){ 
            return word.length()>0 && 
                Character.isLetter(word.charAt(0))
                && Character.isLetter(word.charAt(word.length()-1));
        }

        /*
         * irelevant - does not appear in the Gold Standard.
         */
        public boolean isRelevantLexema(String word){
            return relevantWords.contains(word.toLowerCase());
        }


        @Override
        //lineExample = "abounded\tabounded/VBD/ccomp/0 in/IN/prep/1 large/JJ/amod/4 tortoise/NN/pobj/2\t102\t
        //1855,1\t1856,2\t1859,2\t1866,1\t1876,1\t1881,1...."
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // a trick to get only the (0)root, (1)edges and (2)count. substring from 0 to the third tab index;
            int ttabI=line.indexOf("\t",line.indexOf("\t", line.indexOf("\t")+1)+1);
            String[] parts = line.substring(0,ttabI).split("\t"); 
            if(isRelevantLexema(parts[0])){
                String[] edges = parts[1].split("\\s+");
                for(String e : edges){
                    String[] edgeComposits = e.split("/");
                    // 'target/root word' = parts[0] , 'dependant word' = eComp[0] , 'relation' = eComp[2] , 'count' = parts[2]
                    if(isLegalWord(edgeComposits[0])){
                        // for count(F=f,L=l)
                        wKey = new Key1(parts[0].toLowerCase(),edgeComposits[0].toLowerCase(),edgeComposits[2].toLowerCase());
                        context.write(wKey, new LongWritable(Long.valueOf(parts[2])));
                        // for count(F=f)
                        wKey = new Key1(Consts.STAR,edgeComposits[0].toLowerCase(),edgeComposits[2].toLowerCase());
                        context.write(wKey, new LongWritable(Long.valueOf(parts[2])));
                        // for count(L=l)
                        wKey = new Key1(parts[0].toLowerCase(),Consts.STAR,Consts.STAR);
                        context.write(wKey, new LongWritable(Long.valueOf(parts[2])));
                    }
                }
            }
            // for count(L) calc
            // the biarcs are lexicographically sorted by part[0] first,
            // hence this calculation method is valid.
            if(!parts[0].toLowerCase().equals(latestRoot)){
                countL++;
                latestRoot=parts[0].toLowerCase();
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Key1 countLKey = new Key1(Consts.STAR,Consts.STAR,Consts.STAR);
            context.write(countLKey,new LongWritable(countL));
            super.cleanup(context);
        }

    }
    




    /*
     * the combiner is a local aggregation optimization
     * it is used to reduce the amount of data that is sent to the reducer
     * by reducing locally in the map nodes, before sending the data to the partitioner
    */
    public static class Combiner1 extends Reducer<Key1, LongWritable, Key1, LongWritable> {
        @Override
        public void reduce(Key1 key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }




    public static class Partitioner1 extends Partitioner<Key1, LongWritable> {
        @Override
        public int getPartition(Key1 key, LongWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }



    
    public static class Reducer1 extends Reducer<Key1,LongWritable,Key1,LongWritable> {

        // since we only output relevant words, which are of O(GS_COUPLES_AMOUNT)
        // we can save the count(L=l) counts in memory
        private final HashMap<String,Long> countLel = new HashMap<>();
        boolean isFirstRun = true, isLelMapper = false, writeLel=false;;

        
        @Override
        public void reduce(Key1 key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }

            // for count(L)
            if(key.getRoot().equals(Consts.STAR) && key.getDependant().equals(Consts.STAR) && key.getLabel().equals(Consts.STAR)){
                S3Methods.uploadCountL(sum);
                return;
            }

            if(isFirstRun){
                isLelMapper = key.getDependant().equals(Consts.STAR) && key.getLabel().equals(Consts.STAR);
                writeLel = isLelMapper;
                isFirstRun = false;
            }
            // upload L=l counts.
            if(isLelMapper && key.getDependant().equals(Consts.STAR) && key.getLabel().equals(Consts.STAR)){
                countLel.put(key.getRoot(),sum);
                return;
            }
            // if finished count(L=l)'s
            if(isLelMapper && !key.getDependant().equals(Consts.STAR))
                isLelMapper=false;
            
            context.write(key, new LongWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if(writeLel)
                S3Methods.uploadLel(countLel);
            super.cleanup(context);
        }

    }
    




    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1: Count");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Mapper1.class);
        job.setPartitionerClass(Partitioner1.class);
        job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(Key1.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Key1.class);
        job.setOutputValueClass(LongWritable.class);

        //for running on AWS Emr:
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(Consts.STEP1_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(Consts.STEP1_OUTPUT));

        //for testing locally:
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        //FileInputFormat.addInputPath(job, new Path("test.txt"));
        //TextOutputFormat.setOutputPath(job, new Path("output_test"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    

}