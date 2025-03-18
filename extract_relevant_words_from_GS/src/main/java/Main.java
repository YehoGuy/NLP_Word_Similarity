import java.io.*;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Scanner;

public class Main {
	public static void main(String[] args){
		HashSet<String> visited = new HashSet<>();
		final String relevantWordsFileName = "uniqueGSWords.txt";
		final String joinIdsFileName = "joinIds.txt";
		final PriorityQueue<String> lexicographicallyOrdered = new PriorityQueue<>();
		// path to Golden Standard file
		String pathToFile = "";
		try(Scanner k = new Scanner(System.in)){
			System.out.print("Enter file path (relative): ");
			pathToFile = k.next();
		} catch(Exception e){
			System.out.println("Invalid path argument. Terminating...");
		}
		// create relevant words output file
		try{
			File file = new File(relevantWordsFileName);
			if (file.createNewFile()) {
				System.out.println("[Success] File created successfully: " + file.getAbsolutePath());
				System.out.println(" ---- Starting Parse ----");
			} else {
				System.out.println("[Problem] File named " + relevantWordsFileName + " already exists.");
				return;
			}
		} catch (Exception e) {
			System.out.println("[Error] an error occured while attempting to create output file. "+e);
		}
		// parse
		try (BufferedReader reader = new BufferedReader(new FileReader(pathToFile));
			 BufferedWriter writer = new BufferedWriter(new FileWriter(relevantWordsFileName))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] lineParts = line.split("\t");
				// handle first word
				if(!visited.contains(lineParts[0])) {
					visited.add(lineParts[0]);
					writer.write(lineParts[0] + "\n");
					lexicographicallyOrdered.add(lineParts[0]);
				}
				// handle second word
				if(!visited.contains(lineParts[1])) {
					visited.add(lineParts[1]);
					writer.write(lineParts[1] + "\n");
					lexicographicallyOrdered.add(lineParts[1]);
				}
			}
		} catch (Exception e) {
			System.err.println("Error Parsing file: "+e);
		}


	}

}
