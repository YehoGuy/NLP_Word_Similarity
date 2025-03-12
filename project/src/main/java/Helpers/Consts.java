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

	public static final String relevantWordsS3Key = "relevantWords.txt";

}