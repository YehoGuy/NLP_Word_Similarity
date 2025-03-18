package Helpers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Methods {
    public static void uploadCountL(long countL) throws IOException {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
        String sumString = String.valueOf(countL);
        InputStream stream = new ByteArrayInputStream(sumString.getBytes());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(sumString.getBytes().length);
        PutObjectRequest request = new PutObjectRequest(Consts.BUCKET, Consts.COUNT_L_S3_KEY, stream ,metadata);
        s3.putObject(request);
    }

    public static long retrieveCountL() throws IOException{
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		S3Object s3Object = s3.getObject(new GetObjectRequest(Consts.BUCKET, Consts.COUNT_L_S3_KEY));
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))){
		    return Long.parseLong(reader.readLine());
        }
    }

    public static void uploadLel(HashMap<String,Long> Lel) throws IOException{
        File file = new File("Lelulul");
		if (file.createNewFile()) {
			System.out.println("[Success] File created successfully: " + file.getAbsolutePath());
			System.out.println(" ---- Starting Parse ----");
			try(BufferedWriter writer = new BufferedWriter(new FileWriter(file))){
			    for(Entry<String,Long> e : Lel.entrySet())
				    writer.write(e.getKey() + "\t" + e.getValue() + "\n");
            }
            // Upload the file to S3
            AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
            PutObjectRequest request = new PutObjectRequest(Consts.BUCKET, Consts.LEL_S3_KEY, file);
            s3.putObject(request);
            System.out.println("[Success] File uploaded to S3: " + Consts.LEL_S3_KEY);
		} else {
			System.out.println("[Problem] File named " + Consts.LEL_S3_KEY + " already exists.");
		}     
    }

    public static HashMap<String,Long> retrieveLel() throws IOException{
        HashMap<String,Long> Lel = new HashMap<>();
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		S3Object s3Object = s3.getObject(new GetObjectRequest(Consts.BUCKET, Consts.LEL_S3_KEY));
		try(BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))){
            String line;
		    while ((line = reader.readLine()) != null){
                String[] parts = line.split("\t");
                Lel.put(parts[0], Long.valueOf(parts[1]));
            }
        }
        return Lel;        
    }


    public static HashMap<String,Integer> retrieveJoinIds() throws IOException{
        HashMap<String,Integer> joinIds = new HashMap<>();
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		S3Object s3Object = s3.getObject(new GetObjectRequest(Consts.BUCKET, Consts.JOINIDS_S3_KEY));
		try(BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))){
            String line;
		    while ((line = reader.readLine()) != null){
                String[] parts = line.split("\t");
                joinIds.put(parts[0].trim(), Integer.valueOf(parts[1].trim()));
            }
        }
        return joinIds;        
    }

    public static void uploadPairsCheck(HashSet<String> pairs) throws IOException{
        File file = new File("pairsCheck.txt");
		if (file.createNewFile()) {
			System.out.println("[Success] File created successfully: " + file.getAbsolutePath());
			System.out.println(" ---- Starting Parse ----");
			try(BufferedWriter writer = new BufferedWriter(new FileWriter(file))){
			    for(String pair : pairs)
                    writer.write(pair + "\n");
            }
            // Upload the file to S3
            AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
            PutObjectRequest request = new PutObjectRequest(Consts.BUCKET, "pairsCheck.txt", file);
            s3.putObject(request);
            System.out.println("[Success] File uploaded to S3: " + "pairsCheck.txt");
		} else {
			System.out.println("[Problem] File named " + "pairsCheck.txt" + " already exists.");
		}     
    }

}
