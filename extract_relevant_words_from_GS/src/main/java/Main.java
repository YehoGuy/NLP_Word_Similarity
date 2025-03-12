import java.io.*;
import java.util.HashSet;
import java.util.Scanner;

public class Main {
	public static void main(String[] args){
		HashSet<String> visited = new HashSet<>();
		final String outputFileName = "uniqueGSWords.txt";
		// path to Golden Standard file
		String pathToFile = "";
		try(Scanner k = new Scanner(System.in)){
			System.out.print("Enter file path (relative): ");
			pathToFile = k.next();
		} catch(Exception e){
			System.out.println("Invalid path argument. Terminating...");
		}
		// create output file
		try{
			File file = new File(outputFileName);
			if (file.createNewFile()) {
				System.out.println("[Success] File created successfully: " + file.getAbsolutePath());
				System.out.println(" ---- Starting Parse ----");
			} else {
				System.out.println("[Problem] File named " + outputFileName + " already exists.");
				return;
			}
		} catch (Exception e) {
			System.out.println("[Error] an error occured while attempting to create output file. "+e);
		}
		// parse
		try (BufferedReader reader = new BufferedReader(new FileReader(pathToFile));
			 BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] lineParts = line.split("\t");
				// handle first word
				if(!visited.contains(lineParts[0])) {
					visited.add(lineParts[0]);
					writer.write(lineParts[0] + "\n");
				}
				// handle second word
				if(!visited.contains(lineParts[1])) {
					visited.add(lineParts[1]);
					writer.write(lineParts[1] + "\n");
				}
			}
		} catch (Exception e) {
			System.err.println("Error Parsing file: "+e);
		}

	}

}
