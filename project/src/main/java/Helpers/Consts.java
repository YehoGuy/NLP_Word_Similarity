package Helpers;
import org.apache.hadoop.io.Text;
/*
 * This class contains all the constant static data that is
 * necessary in the project.
 */

public class Consts {
	public static final String STAR = "*";
	public static final Text TEXT_STAR = new Text("*");

	public static final String STEP1_JAR = "s3://bucket1205045/jars/Step1.jar";
	public static final String STEP1_INPUT = "s3://bucket1205045/input/";
	
	public static final String STEP1_OUTPUT = "s3://bucket1205045/output/step1/";

	public static final String STEP1_MEGA_INPUT = "https://biarcs.s3.amazonaws.com/";
	public static final String STEP1_MEGA_OUTPUT = "s3://bucket1205045/mega_output/step1/";

	public static final String STEP2_JAR = "s3://bucket1205045/jars/Step2.jar";
	public static final String STEP2_INPUT = STEP1_OUTPUT;
	public static final String STEP2_OUTPUT = "s3://bucket1205045/output/step2/";

	public static final String STEP3_JAR = "s3://bucket1205045/jars/Step3.jar";
	public static final String STEP3_INPUT = STEP2_OUTPUT;
	public static final String STEP3_OUTPUT = "s3://bucket1205045/output/step3/";


	public static final String LOGS = "s3://bucket1205045/logs/";
	public static final String BUCKET = "bucket1205045";

	public static final String RELEVANT_WORDS_S3_KEY = "uniqueGSWords.txt";

	public static final String COUNT_L_S3_KEY = "vars/countL";

	public static final String LEL_S3_KEY = "vars/Lel";

	public static final String JOINIDS_S3_KEY = "joinIds.txt";

	public static final String GS_S3_KEY = "GS.txt";


	/*
	 * In the classic Stanford Dependencies (around 2010–2013 releases)—
	 * the era Google used—there were typically 55 core named relations. 
	 * source: trust me bro.
	 * just kidding - Official download page for the dataset: Google Books Syntactic N-grams
	 *	Original 2013 announcement: “We present a new dataset of billions of dependency parse structures from scanned books from 1520 to 2008.”
	 */
	public static final int NUMBER_OF_DEPENDENCY_LABELS = 55;

}