package Helpers;
import org.apache.hadoop.io.Text;
/*
 * This class contains all the constant static data that is
 * necessary in the project.
 */

public class Consts {
	public static final String STAR = "*";
	public static final Text TEXT_STAR = new Text("*");

	public static final String STEP1_JAR = "s3://bucket120504/jars/Step1.jar";

	public static final String STEP1_INPUT = "s3://bucket120504/input/";
	public static final String STEP1_OUTPUT = "s3://bucket120504/output/step1/";

	public static final String LOGS = "s3://bucket120504/logs/";
	public static final String BUCKET = "bucket120504";

	public static final String RELEVANT_WORDS_S3_KEY = "relevantWords.txt";

	public static final String COUNT_L_S3_KEY = "vars/countL";

	/*
	 * In the classic Stanford Dependencies (around 2010–2013 releases)—
	 * the era Google used—there were typically 55 core named relations. 
	 * source: trust me bro.
	 * jk - Official download page for the dataset: Google Books Syntactic N-grams
	 *	Original 2013 announcement: “We present a new dataset of billions of dependency parse structures from scanned books from 1520 to 2008.”
	 */
	public static final int numberOfDependancyLabels = 55;

}