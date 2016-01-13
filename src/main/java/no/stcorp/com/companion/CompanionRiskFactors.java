package no.stcorp.com.companion;

import no.stcorp.com.companion.database.*;
import no.stcorp.com.companion.kml.*;
import no.stcorp.com.companion.traffic.*;
import no.stcorp.com.companion.weather.*;
import no.stcorp.com.companion.xml.*;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import org.apache.commons.cli.*;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.*;

import java.time.*;
import java.time.format.*;

import java.util.*;
import java.util.Map.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CompanionRiskFactors {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static SparkConf conf;

  public static JavaSparkContext sc;

  /** 
   * Run some Spark examples for checking if Spark functions as expected
   */ 
  public static void runSparkExamples() {
    String logFile = "/home/osboxes/Tools/spark-1.5.1/README.md"; // Just an arbitrary file 
    /**
     * Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection 
     * of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing 
     * collection in your driver program, or referencing a dataset in an external storage system, such as a shared 
     * filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
     */
    JavaRDD<String> logData = sc.textFile(logFile).cache(); // Read the file logFile as a collection of lines (example of referencing a dataset in an external storage system). 
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
    JavaRDD<Double> distData = sc.parallelize(data); // (Example of parallelizing an existing collection)
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

  /**
   * Download traffic current measurements from NDW. Is used for checking if the measurement sites are all
   * present in the database. If not they will be added.
   */
  public static void getTrafficNDWCurrentMeasurements(String ftpUrl) {
    // Current measurements for getting the measurement sites
    // Download data from NDW
    System.out.println("Trying to download gzip file containing measurements from NDW");
    String rootDir = SparkFiles.getRootDirectory(); // Get the location where the files added with addFile are downloaded
    System.out.println("Gzipped files will be downloaded to: " + rootDir);

    System.out.println("Downloading current measurements containing the measurement locations");
    String measurementFileName = "measurement_current_2016_01_08_16_32_35_779.xml.gz";
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
          JavaRDD<List<MeasurementSite>> measurementSites = gzData.map(new TrafficNDWCurrentMeasurementParser()); // Lazy, i.e. not executed before really needed
          //System.out.println("Putting app to sleep for 10 seconds to see xml parsing not started yet");
          Thread.sleep(100);
          System.out.println("Starting to parse current measurement xml now");

          // int nrOfRecords = siteMeasurements.reduce(new CountXmlRecords());
          System.out.println("Total number of measurement site records: " + measurementSites.count()); // Only here the ParseTrafficSpeedXml is actually called and executed due to the count action
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

  /**
   * Get the speed traffic data from the NDW site
   */
  public static Map<String, List<SiteMeasurement>> getTrafficNDWSpeed(String ftpUrl, String ndwIdPattern, Instant startDate, Instant endDate) {
    Map<String, List<SiteMeasurement>> measurementsPerSite = new HashMap<String, List<SiteMeasurement>>();

    System.out.println("Downloading traffic speed");
    /**
     * Find the appropriate directory: before January 14th, data were downloaded manually as historical data, providing different 
     * naming conventions as well as different files. From the 14th on the actual traffic data is downloaded automatically.
     * < 20160114: 
     *      Folder name: dd-MM-yyyy
     *      Files: HHmm_Traveltime.gz, HHmm_Trafficspeed.gz
     * >= 20160114: 
     *      Folder name: yyyy_MM_dd
     *      Files: wegwerkzaamheden_yyyy_MM_dd_HH_mm_ss_sss.xml.gz, trafficspeed_yyyy_MM_dd_HH_mm_ss_sss.xml.gz, 
     *             traveltime_yyyy_MM_dd_HH_mm_ss_sss.xml.gz, srti_yyyy_MM_dd_HH_mm_ss_sss.xml.gz,
     *             measurement_current_yyyy_MM_dd_HH_mm_ss_sss.xml.gz, gebeurtenisinfo_yyyy_MM_dd_HH_mm_ss_sss.xml.gz,
     *             incidents_yyyy_MM_dd_HH_mm_ss_sss.xml.gz, measurements_yyyy_MM_dd_HH_mm_ss_sss.xml.gz,
     *             brugopeningen_yyyy_MM_dd_HH_mm_ss_sss.xml.gz
     */
    String trafficFileName = "trafficspeed_2016_01_08_16_32_38_230.xml.gz";
    String trafficZipUrl = ftpUrl + trafficFileName;
    System.out.println("Traffic zip URL: " + trafficZipUrl);
    sc.addFile(trafficZipUrl);
    String trafficFilePath = SparkFiles.get(trafficFileName);
    System.out.println("Traffic file path: " + trafficFilePath);

    JavaRDD<String> gzData = sc.textFile(trafficFilePath).cache(); // textFile should decompress gzip automatically
    //System.out.println("Output: ");
    //System.out.println(gzData.toString());
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
      String importedFileText = gzDataList.get(0);
      TrafficNDWSpeedParser parser = new TrafficNDWSpeedParser();
      List<SiteMeasurement> measurements = parser.call(importedFileText);
      JavaRDD<SiteMeasurement> measurementsDistributed = sc.parallelize(measurements); // Parallelizing the existing collection
      System.out.println("Number of speed measurements: " + measurementsDistributed.count());
      DatabaseManager dbMgr = DatabaseManager.getInstance();
      List<String> ndwIds = dbMgr.getNdwIdsFromNdwIdPattern(ndwIdPattern);
      // Now determine the measurements per station
      for (String ndwId : ndwIds) {
        // Filter based on ndw id and then collect into a list
        List<SiteMeasurement> measurementsForSite = measurementsDistributed.filter(ms -> ms.getMeasurementSiteReference().equalsIgnoreCase(ndwId)).collect();
        measurementsPerSite.put(ndwId, measurementsForSite);
      }
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

    // try {
    //   System.out.println("Putting app to sleep for 10 seconds again");
    //   Thread.sleep(10000);
    // } catch (InterruptedException ex) {
    //   System.out.println("Something went wrong putting the app to sleep for 10 seconds again");
    //   ex.printStackTrace();
    //   Thread.currentThread().interrupt();
    // }
    return measurementsPerSite;
  }

  /**
   * Get the weather observations from the KNMI website. Is used for checking if all the weather
   * stations are present in the database. If not, they will be added.
   * Both startDate and endDate in format yyyyMMddHH 
   * HH represents the hour that was just finished (i.e. 15 implies 14.00 - 15.00)
   */
  public static Map<String, List<String>> getWeatherKNMIObservations(String ndwIdPattern, String startDate, String endDate) {
    System.out.println("Downloading weather for ndw id pattern: " + ndwIdPattern + ", start date: " + startDate + ", end date: " + endDate);
    Map<String, List<String>> weatherObservationsForMeasurementSite = new HashMap<String, List<String>>();
    String url = "http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi";
    HttpClient client = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost(url);
    String USER_AGENT = "Mozilla/5.0";
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("start", startDate));
    urlParameters.add(new BasicNameValuePair("end", endDate));
    urlParameters.add(new BasicNameValuePair("vars", "ALL"));
    urlParameters.add(new BasicNameValuePair("stns", "ALL"));
    post.setHeader("User-Agent", USER_AGENT);
    String content = String.format("");
    try {
      post.setEntity(new UrlEncodedFormEntity(urlParameters)); 
      //post.setEntity(new StringEntity(content));
      HttpResponse response = client.execute(post);
      System.out.println("HttpResponse status code: " + response.getStatusLine().getStatusCode() + ", reason phrase: " + response.getStatusLine().getStatusCode());
      InputStream is = response.getEntity().getContent();
      String weatherResponse = readInputStream(is);
      Instant now = Instant.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault());
      File tmpFile = File.createTempFile("WeatherResponse_" + formatter.format(now), ".tmp");
      BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile));
      writer.write(weatherResponse);
      writer.close();
      JavaRDD<String> weatherData = sc.textFile(tmpFile.getAbsolutePath()).cache();
      System.out.println("Weather data count before parsing: " + weatherData.count());
      //System.out.println(readInputStream(is));
      JavaRDD<String> weatherObservationsNotFormatted = weatherData.map(new WeatherKNMIParser());
      // Now write to file again to let the parser create a proper paralellizable list of observations
      //System.out.println("Number of weather observations: " + weatherObservations.count());
      File tmpFileObservations = File.createTempFile("WeatherResponse_Observations_" + formatter.format(now), ".tmp");
      BufferedWriter writerObservations = new BufferedWriter(new FileWriter(tmpFileObservations));
      for (String wo : weatherObservationsNotFormatted.collect()) {
        writerObservations.write(wo);
      }
      writerObservations.close();
      JavaRDD<String> weatherObservations = sc.textFile(tmpFileObservations.getAbsolutePath()).cache();
      System.out.println("Weather data count after parsing: " + weatherObservations.count());

      EntityUtils.consume(response.getEntity()); // To make sure everything is properly released

      // Get the weather and traffic for a traffic measurement point (or more than one). Currently matching a NDW id pattern.
      DatabaseManager dbMgr = DatabaseManager.getInstance();
      Map<String, Integer> weatherStationForMeasurementPoints = dbMgr.getWeatherStationForMeasurementPoints(ndwIdPattern);
      for (Entry<String, Integer> wsMp : weatherStationForMeasurementPoints.entrySet()) {
        int selectedStationNdwId = wsMp.getValue();
        System.out.println("Measurement point " + wsMp.getKey() + " gets weather from weather station " + selectedStationNdwId + "; getting weather observations for this station");
        List<String> weatherObservationsForStation = weatherObservations.filter(new Function<String, Boolean>() {
          private static final long serialVersionUID = 7L;
          public Boolean call(String s) { 
              return s.trim().startsWith(String.valueOf(selectedStationNdwId));
          }
        }).collect();
        weatherObservationsForMeasurementSite.put(wsMp.getKey(), weatherObservationsForStation);
        // for (String obs : weatherObservationsForStation) {
        //   System.out.println(obs);
        // }
        // System.out.println("-----------------------------------------");
      }
    } catch (IOException ex) {
      System.out.println("Something went wrong trying to save downloaded weather data to file;" + ex.getMessage());
      ex.printStackTrace();
    } catch (Exception ex) {
      System.out.println("Something went wrong trying to download and parse weather data;" + ex.getMessage());
      ex.printStackTrace();
    }
    System.out.println("Finished with weather");
    try {
      System.out.println("Putting app to sleep for 10 seconds after weather actions");
      Thread.sleep(10000);
    } catch (InterruptedException ex) {
      System.out.println("Something went wrong putting the app to sleep for 100 seconds again");
      ex.printStackTrace();
      Thread.currentThread().interrupt();
    }
    return weatherObservationsForMeasurementSite;
  }

  public static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("The following flags are available:", options);
  }

  /**
   * Run this program as follows: 
   * /home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "no.stcorp.com.companion.CompanionRiskFactors" 
   *    --jars /home/osboxes/.m2/repository/org/postgresql/postgresql/9.4-1206-jdbc42/postgresql-9.4-1206-jdbc42.jar,
   *    /home/osboxes/.m2/repository/org/apache/httpcomponents/httpclient/4.5.1/httpclient-4.5.1.jar,
   *    /home/osboxes/.m2/repository/org/apache/httpcomponents/httpcore/4.4.4/httpcore-4.4.4.jar,
   *    /home/osboxes/.m2/repository/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar --master local[*] 
   *    target/CompanionWeatherTraffic-0.1.jar -se 
   *
   *    or one of the options: -tcm -ts -wo -link -kml -proc
   *
   * Note the --jars to indicate the additional jars that need to be loaded 
   * The driver-memory can be set to a larger value than the default 1g to avoid Java heap space problems
   */
  public static void main(String[] args) {
    /**
     * A Spark configuration object with a name for the application. The master (a Spark, Mesos or YARN cluster URL) 
     * is not set as it will be obtained by launching the application with spark-submit.
     */
    conf = new SparkConf().setAppName("COMPANION Weather Traffic Change Detection System")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.maxResultSize", "3g"); 
      /**
       * The previous settings do not work when running in local mode:
       * For local mode you only have one executor, and this executor is your driver, so you need to set the driver's 
       * memory instead. That said, in local mode, by the time you run spark-submit, a JVM has already been launched 
       * with the default memory settings, so setting "spark.driver.memory" in your conf won't actually do anything for 
       * you. Instead use something like the following to set the driver memory for local mode:
       * /home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "CompanionRiskFactors" --master local[*] target/CompanionWeatherTraffic-0.1.jar
       */
    sc = new JavaSparkContext(conf); // JavaSparkContext object tells Spark how to access a cluster
    String ftpUrl = "ftp://83.247.110.3/"; // Old URL valid until 2016/01/15
    ftpUrl = "ftp://opendata.ndw.nu/"; // New URL valid from 2016/01/01 (15 days overlap)
    ftpUrl = "ftp://companion:1d1ada@192.168.1.33/Projects/companion/downloadedData/NDW/2016_01_08/"; // Data downloaded locally due to awkard interface for downloading historical data on NDW

    Options options = new Options();
    options.addOption("se", false, "Run some Spark examples to see if Spark is functioning as expected");
    options.addOption("tcm", false, "Get current traffic measurements from NDW (containing measurement sites)");
    options.addOption("ts", false, "Get speed measurements from NDW");
    options.addOption("wo", false, "Get weather observations from KNMI");
    options.addOption("link", false, "Link measurement sites with weather observations");
    options.addOption("kml", false, "Generate KML files for the weather stations and measurement sites");
    options.addOption("proc", false, "Run a processing sequence: fetch weather and traffic data ...");

    if (args.length == 0) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.err.println("No arguments provided when running the program.");
      printHelp(options);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    }
    try {
      CommandLineParser parser = new PosixParser(); // Should be using the DefaultParser, but this is generating an exception: Exception in thread "main" java.lang.IllegalAccessError: tried to access method org.apache.commons.cli.Options.getOptionGroups()Ljava/util/Collection; from class org.apache.commons.cli.DefaultParser
      CommandLine cmd = parser.parse(options, args, true);

      String ndwIdPattern = "RWS01_MONIBAS_0131hrl00%";
      String startDateString = "20160108150000";
      String endDateString = "20160108170000";
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault());
      Instant startDate = formatter.parse(startDateString, ZonedDateTime::from).toInstant();
      Instant endDate = formatter.parse(endDateString, ZonedDateTime::from).toInstant();
      DateTimeFormatter formatterWeatherKNMI = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.systemDefault());
      String startDateStringKNMI = formatterWeatherKNMI.format(startDate);
      String endDateStringKNMI = formatterWeatherKNMI.format(endDate);
      System.out.println("Start date KNMI: " + startDateStringKNMI + " - end date KNMI: " + endDateStringKNMI);
      try {
        System.out.println("Putting app to sleep for 10 seconds again");
        Thread.sleep(10000);
      } catch (InterruptedException ex) {
        System.out.println("Something went wrong putting the app to sleep for 10 seconds again");
        ex.printStackTrace();
        Thread.currentThread().interrupt();
      }

      if (cmd.hasOption("se")) {
        runSparkExamples();
      } else if (cmd.hasOption("tcm")) {
        getTrafficNDWCurrentMeasurements(ftpUrl);
      } else if (cmd.hasOption("ts")) {
        Map<String, List<SiteMeasurement>> speedMeasurements = getTrafficNDWSpeed(ftpUrl, ndwIdPattern, startDate, endDate);
        for (Entry<String, List<SiteMeasurement>> speedEntry : speedMeasurements.entrySet()) {
          String ndwId = speedEntry.getKey();
          List<SiteMeasurement> sms = speedEntry.getValue();
          System.err.println("=================================================");
          System.out.println("Measurement site: " + ndwId);
          System.out.println("  Traffic:");
          for (SiteMeasurement sm : sms) {
            System.out.println("    " + sm);
          }
        }
      } else if (cmd.hasOption("wo")) {
        getWeatherKNMIObservations(ndwIdPattern, startDateStringKNMI, endDateStringKNMI);
      } else if (cmd.hasOption("link")) {
        // Add a link between all measurement sites and weather stations. Only needs to be done when measurement site and 
        // weather station tables have been filled without adding these links. Normally not needed to do this.
        DatabaseManager dbMgr = DatabaseManager.getInstance();
        dbMgr.linkAllMeasurementSitesWithClosestWeatherStation();
      } else if (cmd.hasOption("kml")) {
        // Generate KML from database data
        DatabaseManager dbMgr = DatabaseManager.getInstance();
        List<WeatherStation> wsList = dbMgr.getAllWeatherStations();
        KmlGenerator kmlGenerator = new KmlGenerator();
        kmlGenerator.generateKmlForWeatherStations(wsList);
        List<MeasurementSite> msList = dbMgr.getAllMeasurementSites();
        kmlGenerator.generateKmlForMeasurementSites(msList);
        List<MeasurementSite> msAreaList = dbMgr.getMeasurementSitesWithinArea(51.8f, 4.0f, 52.5f, 5.5f);
        kmlGenerator.generateKmlForMeasurementSites(msAreaList);
        List<MeasurementSite> msPatternMatchingList = dbMgr.getMeasurementPointsForNdwidPattern(ndwIdPattern);
        kmlGenerator.generateKmlForMeasurementSites(msPatternMatchingList);
      } else if (cmd.hasOption("proc")) {
        getTrafficNDWCurrentMeasurements(ftpUrl);
        Map<String, List<SiteMeasurement>>  currentSpeedMeasurementsForMeasurementsSites = getTrafficNDWSpeed(ftpUrl, ndwIdPattern, startDate, endDate);
        Map<String, List<String>> weatherObservationsForMeasurementSites = getWeatherKNMIObservations(ndwIdPattern, startDateStringKNMI, endDateStringKNMI);
        for (Entry<String, List<SiteMeasurement>> speedEntry : currentSpeedMeasurementsForMeasurementsSites.entrySet()) {
          String ndwId = speedEntry.getKey();
          List<SiteMeasurement> sms = speedEntry.getValue();
          System.err.println("=================================================");
          System.out.println("Measurement site: " + ndwId);
          if (weatherObservationsForMeasurementSites.containsKey(ndwId)) {
            List<String> wos = weatherObservationsForMeasurementSites.get(ndwId);
            System.out.println("  Traffic:");
            for (SiteMeasurement sm : sms) {
              System.out.println("    " + sm);
            }
            System.out.println("  Weather:");
            for (String wo : wos) {
              System.out.println("    " + wo);
            }
          } else {
            System.err.println("  No weather observations for this measurement site");
          }
          System.err.println("-------------------------------------------------");
        }
      } else {
        System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        System.err.println("No known arguments provided when running the program.");
        printHelp(options);
        System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        System.exit(-1);
      }
    } catch (ParseException ex) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.out.println("Arguments provided cannot be parsed: " + ex.getMessage());
      ex.printStackTrace();
      printHelp(options);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    } catch (Exception ex) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.out.println("Arguments provided not correct: " + ex.getMessage());
      ex.printStackTrace();
      printHelp(options);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    }

  }

  public static String readInputStream(InputStream input) throws IOException {
    try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
      return buffer.lines().collect(Collectors.joining("____"));
    }
  }  

}