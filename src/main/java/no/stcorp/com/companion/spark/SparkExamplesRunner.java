package no.stcorp.com.companion.spark;

import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

/** 
 * Run some Spark examples for checking if Spark functions as expected
 */ 
public class SparkExamplesRunner {

  private static JavaSparkContext mSparkContext;

  /**
   * Constructor
   * @param SparkContext The Spark context needed to load files, parallelize arrays, etc...
   */
	public SparkExamplesRunner(JavaSparkContext pSparkContext) {
		mSparkContext = pSparkContext;
	}

	/**
	 * Run the spark examples
	 */
	public void run() {
    String logFile = "/home/osboxes/Tools/spark-1.5.1/README.md"; // Just an arbitrary file 
    /**
     * Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection 
     * of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing 
     * collection in your driver program, or referencing a dataset in an external storage system, such as a shared 
     * filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
     */
    JavaRDD<String> logData = mSparkContext.textFile(logFile).cache(); // Read the file logFile as a collection of lines (example of referencing a dataset in an external storage system). 
    System.out.println("Logdata lines: " + logData.count());

    // Find the number of lines containing the letters a and b using the filter transformation
    /**
     * All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just 
     * remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed 
     * when an action requires a result to be returned to the driver program.
     *
     * Spark’s API relies heavily on passing functions in the driver program to run on the cluster. In Java, functions 
     * are represented by classes implementing the interfaces in the org.apache.spark.api.java.function package. There 
     * are two ways to create such functions:
     * 1. Implement the Function interfaces in your own class, either as an anonymous inner class or a named one, and 
     *    pass an instance of it to Spark.
     * 2. In Java 8, use lambda expressions to concisely define an implementation.
     */
    long numAs = logData.filter(new Function<String, Boolean>() {
      private static final long serialVersionUID = 5L;
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      private static final long serialVersionUID = 6L;
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    // Just pause the application shortly to see the results of the previous operations
    try {
      System.out.println("Putting app to sleep for 1 second");
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      System.out.println("Something went wrong putting the app to sleep for 1 second");
      ex.printStackTrace();
      Thread.currentThread().interrupt();
    }

    System.out.println("Computing the sum of the elements in an array");
    int nrOfElements = 1000;
    Double[] doubleData = new Double[nrOfElements];
    for (int i=0; i<nrOfElements; i++) {
      doubleData[i] = i * Math.random();
    }

    // Compute the sum of a number of elements in a list
    List<Double> data = Arrays.asList(doubleData); 
    JavaRDD<Double> distData = mSparkContext.parallelize(data); // (Example of parallelizing an existing collection)
    System.out.println("Elements in parallelized collection: " + distData.count());
    double sum = distData.reduce((a, b) -> a + b); 
    System.out.println("Sum of elements: " + sum);

    try {
      System.out.println("Putting app to sleep for 1 second");
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      System.out.println("Something went wrong putting the app to sleep for 1 second");
      ex.printStackTrace();
      Thread.currentThread().interrupt();
    }
	}
	
}