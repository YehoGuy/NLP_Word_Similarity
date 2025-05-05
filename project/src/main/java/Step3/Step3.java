package Step3;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.client.builder.AdvancedConfig;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import Helpers.Consts;
import Helpers.S3Methods;
import Step2.Key2;
import Step2.Map2;
import Step2.Value2;

// reducer side join
public class Step3 {

    public static class Mapper3 extends Mapper<Key2, Map2, Key3, MapWritable> {

        // relevantWords is a datastructure (set) containing only the relevant target words,
        // for optimization - minimizing number of keys outputted from the Mapper.
        private final List<String> relevantWords = new ArrayList<>();

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

        @Override
        public void map(Key2 key, Map2 value, Context context) throws IOException, InterruptedException {
            for(String word : relevantWords){
                Key3 newKey = new Key3(key.getLexama(), word);
                context.write(newKey, value.getMap());
            }
        }
            

    }


    public static class Partitioner3 extends Partitioner<Key3, MapWritable> {
        @Override
        public int getPartition(Key3 key, MapWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }



    
    public static class Reducer3 extends Reducer<Key3,MapWritable,Key3,Value3> {

        private final HashSet<String> relevantPairs = new HashSet<String>();
        
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // retrieve relevant words file and setup the relevantWords Set.
            AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
			S3Object s3Object = s3.getObject(new GetObjectRequest(Consts.BUCKET, Consts.GS_S3_KEY));
			BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
            String line;
			while ((line = reader.readLine()) != null){
                String[] parts = line.split("\t");
                if(parts[0].toLowerCase().compareTo(parts[1].toLowerCase()) < 0)
                    relevantPairs.add(parts[0].toLowerCase().trim()+"\t"+parts[1].toLowerCase().trim());
                else
                    relevantPairs.add(parts[1].toLowerCase().trim()+"\t"+parts[0].toLowerCase().trim());
            }
            S3Methods.uploadPairsCheck(relevantPairs);
                
        }
        
        @Override
        public void reduce(Key3 key, Iterable<MapWritable> values, Context context) throws IOException,  InterruptedException {
            if(relevantPairs.contains(key.toString())){
                try{
                    Iterator<MapWritable> it = values.iterator();
                    MapWritable v1 = it.next();
                    MapWritable v2 = it.next();
                    HashSet<Writable> featureUnion = new HashSet<>(v1.keySet());
                    featureUnion.addAll(v2.keySet());
                    DoubleWritable[] combinationScores = new DoubleWritable[24];
                    for(int associationIndex=0 ; associationIndex<4 ; associationIndex++){
                        combinationScores[6*associationIndex+0] = new DoubleWritable(manhattanDistance(v1, v2, featureUnion, associationIndex));
                        combinationScores[6*associationIndex+1] = new DoubleWritable(euclideanDistance(v1, v2, featureUnion, associationIndex));
                        combinationScores[6*associationIndex+2] = new DoubleWritable(cosineSimilarity(v1, v2, featureUnion, associationIndex));
                        combinationScores[6*associationIndex+3] = new DoubleWritable(jaccardSimilarity(v1, v2, featureUnion, associationIndex));
                        combinationScores[6*associationIndex+4] = new DoubleWritable(diceSimilarity(v1, v2, featureUnion, associationIndex));
                        combinationScores[6*associationIndex+5] = new DoubleWritable(jensenShannonDivergence(v1, v2, featureUnion, associationIndex));
                    }
                    context.write(key, new Value3(combinationScores));
                } catch (Exception e){
                    
                }
            } 
        }

        
        /*
         * associationIndex%4:
         *  0 for frequency
         *  1 for probability
         *  2 for pmi
         *  3 for ttest
         */
        public double manhattanDistance(MapWritable v1, MapWritable v2, HashSet<Writable> featureSet, int associationIndex){
            double sum=0;
            for(Writable feature : featureSet){
                Value2 v1Val2 = ((Value2)v1.getOrDefault(feature, null));
                Value2 v2Val2 = ((Value2)v2.getOrDefault(feature, null));
                double v1Score = v1Val2==null ? 0 : v1Val2.getAssociation(associationIndex);
                double v2Score = v2Val2==null ? 0 : v2Val2.getAssociation(associationIndex);
                sum+=Math.abs(v1Score-v2Score);
            }
            return sum;
        }

        public double euclideanDistance(MapWritable v1, MapWritable v2, HashSet<Writable> featureSet, int associationIndex){
            double sum=0;
            for(Writable feature : featureSet){
                Value2 v1Val2 = ((Value2)v1.getOrDefault(feature, null));
                Value2 v2Val2 = ((Value2)v2.getOrDefault(feature, null));
                double v1Score = v1Val2==null ? 0 : v1Val2.getAssociation(associationIndex);
                double v2Score = v2Val2==null ? 0 : v2Val2.getAssociation(associationIndex);
                sum+=Math.pow(v1Score-v2Score, 2);
            }
            return Math.sqrt(sum);
        }

        public double cosineSimilarity(MapWritable v1, MapWritable v2, HashSet<Writable> featureSet, int associationIndex){
            double dotProduct=0;
            double v1Norm=0;
            double v2Norm=0;
            for(Writable feature : featureSet){
                Value2 v1Val2 = ((Value2)v1.getOrDefault(feature, null));
                Value2 v2Val2 = ((Value2)v2.getOrDefault(feature, null));
                double v1Score = v1Val2==null ? 0 : v1Val2.getAssociation(associationIndex);
                double v2Score = v2Val2==null ? 0 : v2Val2.getAssociation(associationIndex);
                dotProduct+=v1Score*v2Score;
                v1Norm+=Math.pow(v1Score, 2);
                v2Norm+=Math.pow(v2Score, 2);
            }
            try{
                double ans = dotProduct/(Math.sqrt(v1Norm)*Math.sqrt(v2Norm));
                return ans;
            }catch(Exception e){
                System.err.println("[ERROR] "+e.getMessage());
                return 999999;
            }
        }


        public double jaccardSimilarity(MapWritable v1, MapWritable v2, HashSet<Writable> featureSet, int associationIndex){
            double intersection=0;
            double union=0;
            for(Writable feature : featureSet){
                Value2 v1Val2 = ((Value2)v1.getOrDefault(feature, null));
                Value2 v2Val2 = ((Value2)v2.getOrDefault(feature, null));
                double v1Score = v1Val2==null ? 0 : v1Val2.getAssociation(associationIndex);
                double v2Score = v2Val2==null ? 0 : v2Val2.getAssociation(associationIndex);
                intersection+=Math.min(v1Score, v2Score);
                union+=Math.max(v1Score, v2Score);
            }
            try{
                double ans = intersection/union;
                return ans;
            } catch(Exception e){
                System.err.println("[ERROR] "+e.getMessage());
                return 999999;
            }
        }

        public double diceSimilarity(MapWritable v1, MapWritable v2, HashSet<Writable> featureSet, int associationIndex){
            double intersection=0;
            double union=0;
            for(Writable feature : featureSet){
                Value2 v1Val2 = ((Value2)v1.getOrDefault(feature, null));
                Value2 v2Val2 = ((Value2)v2.getOrDefault(feature, null));
                double v1Score = v1Val2==null ? 0 : v1Val2.getAssociation(associationIndex);
                double v2Score = v2Val2==null ? 0 : v2Val2.getAssociation(associationIndex);
                intersection+=Math.min(v1Score, v2Score);
                union+=v1Score+v2Score;
            }
            try {
                double ans = 2*intersection/union;
                return ans;
            } catch (Exception e) {
                System.err.println("[ERROR] "+e.getMessage());
                return 999999;
            }
        }

        public double jensenShannonDivergence(MapWritable v1, MapWritable v2, HashSet<Writable> featureSet, int associationIndex){
            double sum=0;
            for(Writable feature : featureSet){
                Value2 v1Val2 = ((Value2)v1.getOrDefault(feature, null));
                Value2 v2Val2 = ((Value2)v2.getOrDefault(feature, null));
                double v1Score = v1Val2==null ? 0 : v1Val2.getAssociation(associationIndex);
                double v2Score = v2Val2==null ? 0 : v2Val2.getAssociation(associationIndex);
                double avg = (v1Score+v2Score)/2;
                try{
                    sum+=v1Score*Math.log(v1Score/avg)+v2Score*Math.log(v2Score/avg);
                } catch (Exception e){
                    sum+=999999;
                }
                
            }
            return sum/2;
        }



        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            super.cleanup(context);
        }

    }
    




    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3: pair similarity vectors");
        job.setJarByClass(Step3.class);
        job.setMapperClass(Mapper3.class);
        job.setPartitionerClass(Partitioner3.class);
        job.setReducerClass(Reducer3.class);
        job.setMapOutputKeyClass(Key3.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Key3.class);
        job.setOutputValueClass(Value3.class);

        //for running on AWS Emr:
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(Consts.STEP3_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(Consts.STEP3_OUTPUT));

        //for testing locally:
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        //FileInputFormat.addInputPath(job, new Path("test.txt"));
        //TextOutputFormat.setOutputPath(job, new Path("output_test"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
    
    

}