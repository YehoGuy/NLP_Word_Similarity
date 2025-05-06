import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Main {

    // Change these as needed
    private static final int NUM_SIM_SCORES = 24;

    public static void main(String[] args) {
        Scanner k = new Scanner(System.in);
		System.out.println("Enter the input folder path: ");
        String inputFolderPath = k.next();
		System.out.println("Enter the golden standard file path: ");
        String goldStandardFilePath = k.next();
        String outputArffPath = "data.ARFF";
        
        // Read the golden standard file into a map
        Map<String, String> goldMap = readGoldenStandard(goldStandardFilePath);
        
        File inputFolder = new File(inputFolderPath);
        if (!inputFolder.isDirectory()) {
            System.err.println("Input path is not a directory: " + inputFolderPath);
            System.exit(1);
        }
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputArffPath))) {
            // Write ARFF header
            writer.println("@relation semantic_similarity\n");
            writer.println("@attribute lexama1 string");
            writer.println("@attribute lexama2 string");
            for (int i = 1; i <= NUM_SIM_SCORES; i++) {
                writer.println("@attribute sim" + i + " numeric");
            }
            writer.println("@attribute class {similar, not_similar}\n");
            writer.println("@data");
            
            // Process each file in the folder
            File[] files = inputFolder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if(file.isFile()){
                        processFile(file, writer, goldMap);
                    }
                }
            }
            
        } catch(IOException e) {
            e.printStackTrace();
        }
        
        System.out.println("ARFF file created at: " + outputArffPath);
        k.close();
    }
    
    /**
     * Reads the Golden Standard file and returns a map.
     * The key is a canonical string: lexeme1 and lexeme2 (in lower case and sorted) separated by a tab.
     * The value is "similar" if the third column is "True" (ignoring case), otherwise "not_similar".
     */
    private static Map<String, String> readGoldenStandard(String goldStandardFilePath) {
        Map<String, String> goldMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(goldStandardFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if(line.isEmpty()) continue;
                String[] parts = line.split("\t");
                if (parts.length < 3) {
                    System.err.println("Malformed Golden Standard line: " + line);
                    continue;
                }
                String lex1 = parts[0].trim().toLowerCase();
                String lex2 = parts[1].trim().toLowerCase();
                // Create canonical key by sorting lex1 and lex2 (alphabetically)
                String canonicalKey = (lex1.compareTo(lex2) < 0) ? lex1 + "\t" + lex2 : lex2 + "\t" + lex1;
                String label = parts[2].trim();
                String classLabel = label.equalsIgnoreCase("True") ? "similar" : "not_similar";
                goldMap.put(canonicalKey, classLabel);
            }
        } catch(IOException e) {
            System.err.println("Error reading Golden Standard file: " + goldStandardFilePath);
            e.printStackTrace();
        }
        return goldMap;
    }
    
    /**
     * Processes a single similarity file.
     * Each line is expected to be in the format:
     * lexama1<TAB>lexama2<TAB>[simScore1 , simScore2 , ... , simScore24]
     * It uses the goldMap to look up the class label for the pair.
     */
    private static void processFile(File file, PrintWriter writer, Map<String, String> goldMap) {
        System.out.println("Processing file: " + file.getName());
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if(line.isEmpty()) continue;
                
                String[] parts = line.split("\t");
                if (parts.length < 3) {
                    System.err.println("Malformed line (expected 3 parts): " + line);
                    continue;
                }
                
                String lexama1 = parts[0].trim();
                String lexama2 = parts[1].trim();
                String vectorPart = parts[2].trim();
                
                // Remove the square brackets from the vector part
                if(vectorPart.startsWith("[") && vectorPart.endsWith("]")){
                    vectorPart = vectorPart.substring(1, vectorPart.length()-1).trim();
                } else {
                    System.err.println("Malformed vector part in line: " + line);
                    continue;
                }
                
                // Split the similarity scores by comma
                String[] simScoreStrings = vectorPart.split(",");
                if(simScoreStrings.length != NUM_SIM_SCORES) {
                    System.err.println("Expected " + NUM_SIM_SCORES + " similarity scores, but got " 
                            + simScoreStrings.length + " in line: " + line);
                    continue;
                }
                
                // Build the canonical key for golden standard lookup
                String lex1Lower = lexama1.toLowerCase();
                String lex2Lower = lexama2.toLowerCase();
                String canonicalKey = (lex1Lower.compareTo(lex2Lower) < 0) ? lex1Lower + "\t" + lex2Lower : lex2Lower + "\t" + lex1Lower;
                String classLabel = goldMap.getOrDefault(canonicalKey, "?");
                
                // Build ARFF data line
                StringBuilder dataLine = new StringBuilder();
                // Quote string attributes
                dataLine.append("\"").append(lexama1).append("\",");
                dataLine.append("\"").append(lexama2).append("\",");
                // Append similarity scores
                for (int i = 0; i < NUM_SIM_SCORES; i++) {
                    String scoreStr = simScoreStrings[i].trim();
                    dataLine.append(scoreStr);
                    if(i < NUM_SIM_SCORES - 1) {
                        dataLine.append(",");
                    }
                }
                // Append class label
                dataLine.append(",").append(classLabel);
                
                writer.println(dataLine.toString());
            }
        } catch(IOException e) {
            System.err.println("Error processing file: " + file.getAbsolutePath());
            e.printStackTrace();
        }
    }
}
