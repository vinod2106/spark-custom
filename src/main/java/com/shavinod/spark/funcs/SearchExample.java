package com.shavinod.spark.funcs;

import java.net.URL;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchExample {

	// set logger class
	private static final Logger logger = LoggerFactory.getLogger(SearchExample.class);
	private static String inputFilePath;

	/**
	 * This methods accepts a file and prints the no of lines containing string
	 * error.
	 * 
	 * @param sc
	 * @param filePath
	 */
	public static void oldJavaFilter(JavaSparkContext sc, String filePath) {
		// read and filter
		JavaRDD<String> lines = sc.textFile(inputFilePath).filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String s) throws Exception {
				// TODO Auto-generated method stub

				return s.contains("error");
			}
		});

		long numErrors = lines.count();
		logger.info("numErrors ==========> {}", numErrors);

	}

	private static void newJavaFilter(JavaSparkContext sc, String filePath) {
		// read and filter
		JavaRDD<String> lines = sc.textFile(inputFilePath).filter(s -> s.contains("error"));
		long numErrors = lines.count();
		logger.info("numErrors ==========> {}", numErrors);
	}

	public static void main(String[] args) {

		SparkConf sconf = new SparkConf().setAppName("TestFunctions").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sconf);
		sc.setLogLevel("INFO");
		// input file
		String inputFile = args[0];
		// Read the input file name from classLoader. Just give the name of the file not
		// complete path
		// InputStream resourceContent = classLoader.getResourceAsStream(inputFile);
		logger.info("inputFile =======> " + inputFile);

		// for windows and linux
		switch (OsCheck.getOperatingSystemType()) {
		case Windows:
			logger.info("OS =====> Windows detected");
			URL inputFileURL = ClassLoader.getSystemResource(inputFile);
			logger.info("inputFileURL ====> " + inputFileURL.getPath());
			inputFilePath = inputFileURL.getFile();

		case Linux:
			logger.info("Linux detected");
			inputFilePath = inputFile;

		default:
			logger.info("Unkown os detected");
			break;
		}

		newJavaFilter(sc, inputFilePath);

		sc.close();
	}
}
