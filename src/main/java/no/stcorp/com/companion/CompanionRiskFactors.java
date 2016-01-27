package no.stcorp.com.companion;

import no.stcorp.com.companion.aggregate.*;
import no.stcorp.com.companion.database.*;
import no.stcorp.com.companion.kml.*;
import no.stcorp.com.companion.spark.*;
import no.stcorp.com.companion.traffic.*;
import no.stcorp.com.companion.weather.*;
import no.stcorp.com.companion.visualization.*;
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

import org.apache.commons.net.ftp.*;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;

import org.jfree.ui.RefineryUtilities;

import java.io.*;

import java.nio.file.*;

import java.time.*;
import java.time.format.*;
import java.time.temporal.*;

import java.util.*;
import java.util.Map.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CompanionRiskFactors {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("The following flags are available:", options);
  }

  /**
   * @param pArguments The arguments provided to the proc run option. Should generally be a start time and an end time in yyyy-MM-dd HH format
   * @param pStartDate Input/output parameter to be set by this method
   * @param pEndDate Input/output parameter to be set by this method
   * @param The options the program accepts
   * Parse the time arguments of the processing run option
   */
  public static void parseProcessingArguments(String[] pArguments, Instant pStartDate, Instant pEndDate, Options pOptions) {
    if (pArguments.length != 2) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.err.println("The option proc. needs two arguments in the form of times with format yyyy-MM-dd HH separated by a comma. This was not provided. Provided was: " + Arrays.toString(pArguments));
      printHelp(pOptions);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    }
    String startDateString = pArguments[0];
    String endDateString = pArguments[1];
    try {
      DateTimeFormatter cliFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneId.systemDefault());
      pStartDate = cliFormatter.parse(startDateString, ZonedDateTime::from).toInstant();
      pEndDate = cliFormatter.parse(endDateString, ZonedDateTime::from).toInstant();
    } catch (DateTimeException ex) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.out.println("Time should be formatted as yyyy-MM-dd-HH, but format provided was different: " + ex.getMessage());
      ex.printStackTrace();
      printHelp(pOptions);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    }
  }

  /**
   * Run this program as follows: 
   * /home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "no.stcorp.com.companion.CompanionRiskFactors" 
   *     --jars /home/osboxes/.m2/repository/org/postgresql/postgresql/9.4-1206-jdbc42/postgresql-9.4-1206-jdbc42.jar,
   *     /home/osboxes/.m2/repository/org/apache/httpcomponents/httpclient/4.5.1/httpclient-4.5.1.jar,
   *     /home/osboxes/.m2/repository/org/apache/httpcomponents/httpcore/4.4.4/httpcore-4.4.4.jar,
   *     /home/osboxes/.m2/repository/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar 
   *     --master local[*] target/CompanionWeatherTraffic-0.1.jar -proc 2016-01-08-15,2016-01-08-16
   *
   *    or one of the options: -tcm -ts -wo -link -kml -proc -se
   *
   * Note the --jars to indicate the additional jars that need to be loaded 
   * The driver-memory can be set to a larger value than the default 1g to avoid Java heap space problems
   */
  public static void main(String[] args) {
    /**
     * A Spark configuration object with a name for the application. The master (a Spark, Mesos or YARN cluster URL) 
     * is not set as it will be obtained by launching the application with spark-submit.
     */
    SparkConf conf = new SparkConf().setAppName("COMPANION Weather Traffic Change Detection System")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.maxResultSize", "3g")
      .set("spark.files.overwrite", "true"); // This setting to be able to overwrite an existing file on the temp directory as for different days of traffic the file names might be identical 
      /**
       * The previous settings do not work when running in local mode:
       * For local mode you only have one executor, and this executor is your driver, so you need to set the driver's 
       * memory instead. That said, in local mode, by the time you run spark-submit, a JVM has already been launched 
       * with the default memory settings, so setting "spark.driver.memory" in your conf won't actually do anything for 
       * you. Instead use something like the following to set the driver memory for local mode:
       * /home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "CompanionRiskFactors" --master local[*] target/CompanionWeatherTraffic-0.1.jar
       */
    JavaSparkContext sc = new JavaSparkContext(conf); // JavaSparkContext object tells Spark how to access a cluster
    String ftpUrl = "ftp://83.247.110.3/"; // Old URL valid until 2016/01/15
    ftpUrl = "ftp://opendata.ndw.nu/"; // New URL valid from 2016/01/01 (15 days overlap)
    ftpUrl = "ftp://companion:1d1ada@192.168.1.33/Projects/companion/downloadedData/NDW/"; // Data downloaded locally due to awkard interface for downloading historical data on NDW

    Options options = new Options();
    options.addOption("se", false, "Run some Spark examples to see if Spark is functioning as expected");
    options.addOption("tcm", false, "Get current traffic measurements from NDW (containing measurement sites)");
    options.addOption("ts", false, "Get speed measurements from NDW");
    options.addOption("wo", false, "Get weather observations from KNMI");
    options.addOption("link", false, "Link measurement sites with weather observations");
    options.addOption("kml", false, "Generate KML files for the weather stations and measurement sites");
    //options.addOption("proc", false, "Run a processing sequence: fetch weather and traffic data ...");
    Option procOption = OptionBuilder.withDescription("Run a processing sequence: fetch weather and traffic data ... Provide as arguments start and end date in format: yyyy-MM-dd-HH; separate arguments by comma")
                                     .hasArgs(2)
                                     .withValueSeparator(',')
                                     .create("proc");
    options.addOption(procOption);

    if (args.length == 0) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.err.println("No arguments provided when running the program.");
      printHelp(options);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    }
    try {
      // CommandLineParser parser = new PosixParser(); // Should be using the DefaultParser, but this is generating an exception: Exception in thread "main" java.lang.IllegalAccessError: tried to access method org.apache.commons.cli.Options.getOptionGroups()Ljava/util/Collection; from class org.apache.commons.cli.DefaultParser
      CommandLineParser parser = new BasicParser(); // Should be using the DefaultParser, but this is generating an exception: Exception in thread "main" java.lang.IllegalAccessError: tried to access method org.apache.commons.cli.Options.getOptionGroups()Ljava/util/Collection; from class org.apache.commons.cli.DefaultParser
      CommandLine cmd = parser.parse(options, args, true);

      String ndwIdPattern = "RWS01_MONIBAS_0131hrl00%";
      // Times specified in whole hours (weather is not available at higher resolution than that anyway)
      // Default values (used in some - testing - options; will normally be overwritten by option arguments, especially the proc argument)
      String startDateString = "2016010815";
      String endDateString = "2016010816";
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.systemDefault());
      Instant startDate = formatter.parse(startDateString, ZonedDateTime::from).toInstant();
      Instant endDate = formatter.parse(endDateString, ZonedDateTime::from).toInstant();
      DateTimeFormatter formatterWeatherKNMI = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.systemDefault());
      String startDateStringKNMI = formatterWeatherKNMI.format(startDate);
      String endDateStringKNMI = formatterWeatherKNMI.format(endDate);
      System.out.println("Start date KNMI: " + startDateStringKNMI + " - end date KNMI: " + endDateStringKNMI + " (hard coded)");

      if (cmd.hasOption("se")) {
        SparkExamplesRunner ser = new SparkExamplesRunner(sc);
        ser.run();
      } else if (cmd.hasOption("tcm")) {
        TrafficRetrieverNDW trn = new TrafficRetrieverNDW(sc);
        trn.runCurrentMeasurements(ftpUrl);
      } else if (cmd.hasOption("ts")) {
        TrafficRetrieverNDW trn = new TrafficRetrieverNDW(sc);
        Map<String, List<SiteMeasurement>> speedMeasurements = trn.runTrafficNDWSpeed(ftpUrl, ndwIdPattern, startDate, endDate);
        trn.printSiteMeasurementsPerSite(speedMeasurements);
      } else if (cmd.hasOption("wo")) {
        WeatherRetrieverKNMI wrk = new WeatherRetrieverKNMI(sc);
        wrk.run(ndwIdPattern, startDateStringKNMI, endDateStringKNMI);
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
        String[] arguments = cmd.getOptionValues("proc");
        parseProcessingArguments(arguments, startDate, endDate, options);
        startDateString = arguments[0];
        endDateString = arguments[1];
        startDateStringKNMI = formatterWeatherKNMI.format(startDate);
        endDateStringKNMI = formatterWeatherKNMI.format(endDate);
        System.out.println("Start date KNMI: " + startDateStringKNMI + " - end date KNMI: " + endDateStringKNMI + " (from command line)");
        
        TrafficRetrieverNDW trn = new TrafficRetrieverNDW(sc);
        // trn.runCurrentMeasurements(ftpUrl);
        Map<String, List<SiteMeasurement>> currentSpeedMeasurementsForMeasurementsSites = trn.runTrafficNDWSpeed(ftpUrl, ndwIdPattern, startDate, endDate);

        WeatherRetrieverKNMI wrk = new WeatherRetrieverKNMI(sc);
        Map<String, List<String>> weatherObservationsForMeasurementSites = wrk.run(ndwIdPattern, startDateStringKNMI, endDateStringKNMI);

        TrafficWeatherAggregator twa = new TrafficWeatherAggregator();
        twa.getWeatherAndTrafficPerMeasurementSite(currentSpeedMeasurementsForMeasurementsSites, weatherObservationsForMeasurementSites, startDateString, endDateString);
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
    } catch (DateTimeException ex) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.out.println("Time should be formatted as yyyyMMddHH, but format provided was different: " + ex.getMessage());
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

}