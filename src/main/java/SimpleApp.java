/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

public class SimpleApp {
  private static final Pattern SPACE = Pattern.compile(" ");

  /**
   * Run this program as follows: 
   * 
   * /home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "SimpleApp" 
   *     --jars /home/osboxes/.m2/repository/org/postgresql/postgresql/9.4-1206-jdbc42/postgresql-9.4-1206-jdbc42.jar 
   *     --master local[*] target/CompanionWeatherTraffic-0.1.jar
   *
   * Note the --jars to indicate the additional jars that need to be loaded 
   * The driver-memory can be set to a larger value than the default 1g to avoid Java heap space problems
   */
  public static void main(String[] args) {
    String logFile = "/home/osboxes/Tools/spark-1.5.1/README.md"; // Just an arbitrary file 
    /**
     * A Spark configuration object with a name for the application. The master (a Spark, Mesos or YARN cluster URL) 
     * is not set as it will be obtained by launching the application with spark-submit.
     */
    SparkConf conf = new SparkConf().setAppName("Simple Application")
      .set("spark.executor.memory", "5g")
      .set("spark.driver.memory", "5g")
      .set("spark.executor.maxResultSize", "5g"); 
      /**
       * The previous settings do not work when running in local mode:
       * For local mode you only have one executor, and this executor is your driver, so you need to set the driver's 
       * memory instead. That said, in local mode, by the time you run spark-submit, a JVM has already been launched 
       * with the default memory settings, so setting "spark.driver.memory" in your conf won't actually do anything for 
       * you. Instead use something like the following to set the driver memory for local mode:
       * /home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "SimpleApp" --master local[*] target/CompanionWeatherTraffic-0.1.jar
       */
    JavaSparkContext sc = new JavaSparkContext(conf); // JavaSparkContext object tells Spark how to access a cluster
    if (false) {
      /**
       * Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection 
       * of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing 
       * collection in your driver program, or referencing a dataset in an external storage system, such as a shared 
       * filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
       */
      JavaRDD<String> logData = sc.textFile(logFile).cache(); // Read the file logFile as a collection of lines (example of referencing a dataset in an external storage system). 

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
        public Boolean call(String s) { return s.contains("a"); }
      }).count();

      long numBs = logData.filter(new Function<String, Boolean>() {
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
      JavaRDD<Double> distData = sc.parallelize(data); // (Example of parallelizing an existing collection)
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

    // Download data from NDW
    System.out.println("Trying to download gzip file containing measurements from NDW");
    String ftpUrl = "ftp://83.247.110.3/";
    String rootDir = SparkFiles.getRootDirectory(); // Get the location where the files added with addFile are downloaded
    System.out.println("Gzipped files will be downloaded to: " + rootDir);

    // Current measurements
    {
      System.out.println("Downloading current measurements containing the measurement locations");
      String measurementFileName = "measurement_current.gz";
      String measurementZipUrl = ftpUrl + measurementFileName;
      sc.addFile(measurementZipUrl);
      String measurementFilePath = SparkFiles.get(measurementFileName);
      System.out.println("Measurement file path: " + measurementFilePath);

      JavaRDD<String> gzData = sc.textFile(measurementFilePath).cache(); // textFile should decompress gzip automatically
      //System.out.println("Output: " + gzData.toString());
      /**
       * Another common idiom is attempting to print out the elements of an RDD using rdd.foreach(println) or rdd.map(println). 
       * On a single machine, this will generate the expected output and print all the RDD’s elements. However, in cluster mode, 
       * the output to stdout being called by the executors is now writing to the executor’s stdout instead, not the one on the 
       * driver, so stdout on the driver won’t show these! To print all elements on the driver, one can use the collect() method 
       * to first bring the RDD to the driver node thus: rdd.collect().foreach(println). This can cause the driver to run out of 
       * memory, though, because collect() fetches the entire RDD to a single machine; if you only need to print a few elements 
       * of the RDD, a safer approach is to use the take(): rdd.take(100).foreach(println).
       */
      try {
        List<String> gzDataList = gzData.collect(); 
        System.out.println("Number of elements in gzData: " + gzDataList.size());

        // Call the ParseTrafficSpeedXml class which is defined in another class to parse the traffic speed data
        if (gzDataList.size() == 1) {
          try {
            JavaRDD<List<MeasurementSite>> measurementSites = gzData.map(new ParseCurrentMeasurementXml()); // Lazy, i.e. not executed before really needed
            //System.out.println("Putting app to sleep for 10 seconds to see xml parsing not started yet");
            Thread.sleep(100);
            System.out.println("Starting to parse current measurement xml now");

            // int nrOfRecords = siteMeasurements.reduce(new CountXmlRecords());
            System.out.println("Total number of records: " + measurementSites.count()); // Only here the ParseTrafficSpeedXml is actually called and executed due to the count action
          } catch (InterruptedException ex) {
            System.out.println("Something went wrong putting the app to sleep for 10 seconds");
            ex.printStackTrace();
            Thread.currentThread().interrupt();
          } catch (OutOfMemoryError ex) {
            System.out.println("Ran out of memory while mapping the current measurements");
            ex.printStackTrace();
          }
        }
      } catch (OutOfMemoryError ex) {
        System.out.println("Ran out of memory while parsing and counting the current measurements");
        ex.printStackTrace();
      }

      // JavaRDD<String> words = gzData.flatMap(new FlatMapFunction<String, String>() {
      //   @Override
      //   public Iterable<String> call(String s) {
      //     return Arrays.asList(SPACE.split(s));
      //   }
      // });

      // System.out.println("The contents of the unpacked file: ");
      // List<String> wordList = words.collect();
      // System.out.println("    Words collected into the following list: ");
      // for (String word : wordList) {
      //   System.out.println(word);
      // }
      //long sizesOfAllLines = gzData.map(s -> s.length()).reduce((a, b) -> a + b).count();
      //System.out.println("Size: " + sizesOfAllLines);

      try {
        System.out.println("Putting app to sleep for 10 seconds again");
        Thread.sleep(10000);
      } catch (InterruptedException ex) {
        System.out.println("Something went wrong putting the app to sleep for 100 seconds again");
        ex.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    // Traffic speed
    if (false)
    {
      System.out.println("Downloading traffic speed");
      String trafficFileName = "trafficspeed.gz";
      String trafficZipUrl = ftpUrl + trafficFileName;
      sc.addFile(trafficZipUrl);
      String trafficFilePath = SparkFiles.get(trafficFileName);
      System.out.println("Traffic file path: " + trafficFilePath);

      JavaRDD<String> gzData = sc.textFile(trafficFilePath).cache(); // textFile should decompress gzip automatically
      System.out.println("Output: ");
      System.out.println(gzData.toString());
      /**
       * Another common idiom is attempting to print out the elements of an RDD using rdd.foreach(println) or rdd.map(println). 
       * On a single machine, this will generate the expected output and print all the RDD’s elements. However, in cluster mode, 
       * the output to stdout being called by the executors is now writing to the executor’s stdout instead, not the one on the 
       * driver, so stdout on the driver won’t show these! To print all elements on the driver, one can use the collect() method 
       * to first bring the RDD to the driver node thus: rdd.collect().foreach(println). This can cause the driver to run out of 
       * memory, though, because collect() fetches the entire RDD to a single machine; if you only need to print a few elements 
       * of the RDD, a safer approach is to use the take(): rdd.take(100).foreach(println).
       */
      List<String> gzDataList = gzData.collect(); 
      System.out.println("Number of elements in gzData: " + gzDataList.size());

      // Call the ParseTrafficSpeedXml class which is defined in another class to parse the traffic speed data
      if (gzDataList.size() == 1) {
        JavaRDD<List<SiteMeasurement>> siteMeasurements = gzData.map(new ParseTrafficSpeedXml()); // Lazy, i.e. not executed before really needed
        try {
          System.out.println("Putting app to sleep for 10 seconds to see xml parsing not started yet");
          Thread.sleep(10000);
        } catch (InterruptedException ex) {
          System.out.println("Something went wrong putting the app to sleep for 10 seconds");
          ex.printStackTrace();
          Thread.currentThread().interrupt();
        }
        System.out.println("Starting to parse traffic speed xml now");

        // int nrOfRecords = siteMeasurements.reduce(new CountXmlRecords());
        System.out.println("Total number of records: " + siteMeasurements.count()); // Only here the ParseTrafficSpeedXml is actually called and executed due to the count action
      }
      // for (String gzDataElt : gzDataList) {
      //   System.out.println(gzDataElt);
      // }

      // JavaRDD<String> words = gzData.flatMap(new FlatMapFunction<String, String>() {
      //   @Override
      //   public Iterable<String> call(String s) {
      //     return Arrays.asList(SPACE.split(s));
      //   }
      // });

      // System.out.println("The contents of the unpacked file: ");
      // List<String> wordList = words.collect();
      // System.out.println("    Words collected into the following list: ");
      // for (String word : wordList) {
      //   System.out.println(word);
      // }
      //long sizesOfAllLines = gzData.map(s -> s.length()).reduce((a, b) -> a + b).count();
      //System.out.println("Size: " + sizesOfAllLines);

      try {
        System.out.println("Putting app to sleep for 10 seconds again");
        Thread.sleep(10000);
      } catch (InterruptedException ex) {
        System.out.println("Something went wrong putting the app to sleep for 100 seconds again");
        ex.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

  }
}