package no.stcorp.com.companion;

import no.stcorp.com.companion.aggregate.TrafficWeatherAggregator;
import no.stcorp.com.companion.database.DatabaseManager;
import no.stcorp.com.companion.kml.KmlGenerator;
import no.stcorp.com.companion.spark.SparkExamplesRunner;
import no.stcorp.com.companion.traffic.MeasurementSite;
import no.stcorp.com.companion.traffic.MeasurementSiteSegment;
import no.stcorp.com.companion.traffic.SiteMeasurement;
import no.stcorp.com.companion.traffic.TrafficRetrieverNDW;
import no.stcorp.com.companion.visualization.CompanionPlotter;
import no.stcorp.com.companion.weather.WeatherRetrieverKNMI;
import no.stcorp.com.companion.weather.WeatherStation;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileInputStream;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

public class CompanionRiskFactors {
  private static final Pattern SPACE = Pattern.compile(" ");

  private static Properties mCompanionProperties;

  public static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("The following flags are available:", options);
  }

  /**
   * @param pArguments The arguments provided to the proc run option. Should generally be a start time and an end time in yyyy-MM-dd HH format
   * @param pStartDate Input/output parameter to be set by this method
   * @param pEndDate Input/output parameter to be set by this method
   * @param pOptions The options the program accepts
   * @return The parsed start date and end date as Instant objects (first start date followed by end date)
   * Parse the time arguments of the processing run option
   */
  public static List<Instant> parseProcessingArguments(String[] pArguments, Instant pStartDate, Instant pEndDate, Options pOptions) {
    if (pArguments.length != 2) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.err.println("The option proc. needs two arguments in the form of times with format yyyy-MM-dd HH separated by a comma. This was not provided. Provided was: " + Arrays.toString(pArguments));
      printHelp(pOptions);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    }
    String startDateString = pArguments[0];
    String endDateString = pArguments[1];
    List<Instant> returnDates = new ArrayList<Instant>();
    try {
      DateTimeFormatter cliFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneId.systemDefault());
      Instant startDate = cliFormatter.parse(startDateString, ZonedDateTime::from).toInstant();
      Instant endDate = cliFormatter.parse(endDateString, ZonedDateTime::from).toInstant();
      returnDates.add(startDate);
      returnDates.add(endDate);
      System.out.println("Start date: " + startDateString + ", end date: " + endDateString);
    } catch (DateTimeException ex) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.out.println("Time should be formatted as yyyy-MM-dd-HH, but format provided was different: " + ex.getMessage());
      ex.printStackTrace();
      printHelp(pOptions);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    }
    return returnDates;
  }

  public static void setProperties(Properties pProperties) {
    mCompanionProperties = pProperties;
  }

  public static String getProperty(String pProperty) {
    return mCompanionProperties.getProperty(pProperty);
  }

  /**
   * See README.md for an up to date example of how to run this program
   *
   *    or one of the options: -tcm -ts -wo -link -kml -proc -se -export
   *
   * Note the --jars to indicate the additional jars that need to be loaded 
   * The driver-memory can be set to a larger value than the default 1g to avoid Java heap space problems
   *
   */
  public static void main(String[] args) {
    /**
     * A Spark configuration object with a name for the application. The master (a Spark, Mesos or YARN cluster URL) 
     * is not set as it will be obtained by launching the application with spark-submit.
     */

    /**
     * This spark task can be remotely debugged. See http://danosipov.com/?p=779 for details.
     */

    SparkConf conf = new SparkConf().setMaster("local")
      .setAppName("COMPANION Weather Traffic Change Detection System")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.maxResultSize", "3g")
      .set("spark.files.overwrite", "true"); // This setting to be able to overwrite an existing file on the temp directory as for different days of traffic the file names might be identical 

    Properties companionProperties = new Properties();
    try {
      FileInputStream propFile;
      String propPath = "./companion.properties";
      propFile = new FileInputStream(propPath);
      companionProperties.load(propFile);
      propFile.close();
    } catch (Exception ex) {
      System.err.println("Error reading properties file. Make sure the file is called companion.properties and is in the same folder as the executable jar file.");
    }

    CompanionRiskFactors.setProperties(companionProperties);

    /**
     * The previous settings do not work when running in local mode:
     * For local mode you only have one executor, and this executor is your driver, so you need to set the driver's 
     * memory instead. That said, in local mode, by the time you run spark-submit, a JVM has already been launched 
     * with the default memory settings, so setting "spark.driver.memory" in your conf won't actually do anything for 
     * you. Instead use something like the following to set the driver memory for local mode:
     * /home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "CompanionRiskFactors" --master local[*] target/CompanionWeatherTraffic-0.1.jar
     */

    String ndwIdPattern = getProperty("ndw.idPattern");
    String ftpUser = getProperty("ndw.ftp.user");
    String ftpPassword = getProperty("ndw.ftp.password"); 
    String importedFtpUrl = getProperty("ndw.ftp.url"); 
    String ftpFolder = getProperty("ndw.ftp.folder").replaceAll("\\/", "//");

    if (!ftpFolder.startsWith("//"))
      ftpFolder = "//" + ftpFolder;
    
    if (!ftpFolder.endsWith("//"))
      ftpFolder = "//" + ftpFolder;

    String ftpUrl = "ftp://" + ftpUser + ":" + ftpPassword + "@" + importedFtpUrl + ftpFolder;
    System.out.println("NDW FTP: " + ftpUrl);

    String localNdwFolder = getProperty("ndw.localFolder");
    boolean useLocalNdwData = true;
    String useLocalDataString = "";
    try {
      useLocalDataString = getProperty("ndw.useLocalData");
      useLocalNdwData = Boolean.parseBoolean(useLocalDataString);
    } catch (Exception ex) {
      System.err.println("Flag ndw.useLocalData set to invalid value " + useLocalDataString + ", therefore used true instead");
    }

//    String ftpUrl = "ftp://83.247.110.3/"; // Old URL valid until 2016/01/15
//    ftpUrl = "ftp://opendata.ndw.nu/"; // New URL valid from 2016/01/01 (15 days overlap)
//    ftpUrl = "ftp://companion:1d1ada@192.168.1.33/Projects/companion/downloadedData/NDW/"; // Data downloaded locally due to awkard interface for downloading historical data on NDW

    Options options = new Options();
    options.addOption("se", false, "Run some Spark examples to see if Spark is functioning as expected");
//    options.addOption("tcm", false, "Get current traffic measurements from NDW (containing measurement sites)");
    options.addOption("ts", false, "Get speed measurements from NDW");
    options.addOption("wo", false, "Get weather observations from KNMI");
    options.addOption("link", false, "Link measurement sites with weather observations");
    options.addOption("plot", false, "Plot generated data for measurement sites with traffic and weather data");
    options.addOption("kml", false, "Generate KML files for the weather stations and measurement sites");
//    options.addOption("proc", false, "Run a processing sequence: fetch weather and traffic data ...");
    //@SuppressWarnings("deprecation") // For more information on this issue see: https://github.com/HariSekhon/spark-apps/blob/master/build.sbt
    @SuppressWarnings({"deprecation", "static-method"}) 
    Option procOption = OptionBuilder.withDescription("Run a processing sequence: fetch weather and traffic data ... Provide as arguments start and end date in format: yyyy-MM-dd-HH; separate arguments by comma")
                                     .hasArgs(2)
                                     .withValueSeparator(',')
                                     .create("proc");
    options.addOption(procOption);

    Option exportOption = OptionBuilder.withDescription("Run a processing sequence: fetch weather and traffic data and exports to HDF5 file(s) ... Provide as arguments start and end date in format: yyyy-MM-dd-HH; separate arguments by comma")
            .hasArgs(2)
            .withValueSeparator(',')
            .create("export");
    options.addOption(exportOption);
    @SuppressWarnings({"deprecation", "static-method"})
    Option tcmOption = OptionBuilder.withDescription("Get NDW traffic measurements to fill the database with the traffic measurement sites. Provide as arguments the start and end date of the traffic file you are looking for in the format YYYYMMDDHH,YYYYMMDDHH")
                                     .hasArgs(2)
                                     .withValueSeparator(',')
                                     .create("tcm");
    options.addOption(tcmOption);

    if (args.length == 0) {
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.err.println("No arguments provided when running the program.");
      printHelp(options);
      System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
      System.exit(-1);
    }
    try {
      // CommandLineParser parser = new PosixParser(); // Should be using the DefaultParser, but this is generating an exception: Exception in thread "main" java.lang.IllegalAccessError: tried to access method org.apache.commons.cli.Options.getOptionGroups()Ljava/util/Collection; from class org.apache.commons.cli.DefaultParser
      @SuppressWarnings("deprecation")
      CommandLineParser parser = new BasicParser(); // Should be using the DefaultParser, but this is generating an exception: Exception in thread "main" java.lang.IllegalAccessError: tried to access method org.apache.commons.cli.Options.getOptionGroups()Ljava/util/Collection; from class org.apache.commons.cli.DefaultParser
      // For more information on this issue see: https://github.com/HariSekhon/spark-apps/blob/master/build.sbt
      CommandLine cmd = parser.parse(options, args, true);

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

      JavaSparkContext sc = new JavaSparkContext(conf);

      sc.setCheckpointDir("/tmp/spark/");

      System.out.println("-------------Attach debugger now!--------------");

      Thread.sleep(4000);

      if (cmd.hasOption("se")) {
        SparkExamplesRunner ser = new SparkExamplesRunner(conf);
        //ser.run();
        ser.runSvm();

      } else if (cmd.hasOption("tcm")) {
        String[] arguments = cmd.getOptionValues("tcm");
        TrafficRetrieverNDW trn = new TrafficRetrieverNDW(sc, mCompanionProperties);
        startDateString = arguments[0];
        startDate = formatter.parse(startDateString, ZonedDateTime::from).toInstant();
        trn.runCurrentMeasurements(startDate);

      } else if (cmd.hasOption("ts")) {
        TrafficRetrieverNDW trn = new TrafficRetrieverNDW(sc, mCompanionProperties);
        Map<String, List<SiteMeasurement>> speedMeasurements = trn.runTrafficNDWSpeed(ndwIdPattern, startDate, endDate);
        trn.printSiteMeasurementsPerSite(speedMeasurements);

      } else if (cmd.hasOption("wo")) {
        WeatherRetrieverKNMI wrk = new WeatherRetrieverKNMI(sc);
        wrk.run(ndwIdPattern, startDateStringKNMI, endDateStringKNMI);

      } else if (cmd.hasOption("plot")) {
        CompanionPlotter plotter = new CompanionPlotter();
        plotter.plot();

      } else if (cmd.hasOption("link")) {
        // Add a link between all measurement sites and weather stations. Only needs to be done when measurement site and 
        // weather station tables have been filled without adding these links. Normally not needed to do this.
        DatabaseManager dbMgr = DatabaseManager.getInstance();
        dbMgr.linkAllMeasurementSitesWithClosestWeatherStation();

      } else if (cmd.hasOption("kml")) {
        // Generate KML from database data
        DatabaseManager dbMgr = DatabaseManager.getInstance();
        List<WeatherStation> wsList = dbMgr.getAllWeatherStations();
        KmlGenerator kmlGenerator = new KmlGenerator(mCompanionProperties.getProperty("ndw.kmlFolder"));
        kmlGenerator.generateKmlForWeatherStations(wsList);
        List<MeasurementSite> msList = dbMgr.getAllMeasurementSites();
        kmlGenerator.generateKmlForMeasurementSites(msList);
        List<MeasurementSite> msAreaList = dbMgr.getMeasurementSitesWithinArea(52.0f, 4.0f, 52.5f, 5.0f);
        kmlGenerator.generateKmlForMeasurementSites(msAreaList);
        List<MeasurementSite> msPatternMatchingList = dbMgr.getMeasurementPointsForNdwidPattern(ndwIdPattern);
        kmlGenerator.generateKmlForMeasurementSites(msPatternMatchingList);
        List<MeasurementSiteSegment> msSegmentList = dbMgr.getAllMeasurementSitesWithAtLeastTwoCoordinates();
        kmlGenerator.generateKmlForMeasurementSitesWithSegments(msSegmentList);

      } else if (cmd.hasOption("proc")) {
        String[] arguments = cmd.getOptionValues("proc");
        List<Instant> returnDates = parseProcessingArguments(arguments, startDate, endDate, options);
        startDateString = arguments[0];
        endDateString = arguments[1];
        System.out.println("Start date: " + startDateString + ", end date: " + endDateString);
        startDate = returnDates.get(0);
        endDate = returnDates.get(1);
        startDateStringKNMI = formatterWeatherKNMI.format(startDate);
        endDateStringKNMI = formatterWeatherKNMI.format(endDate);
        System.out.println("Start date KNMI: " + startDateStringKNMI + " - end date KNMI: " + endDateStringKNMI + " (from command line)");

        TrafficRetrieverNDW trn = new TrafficRetrieverNDW(sc, mCompanionProperties);
        // trn.runCurrentMeasurements(ftpUrl);
        Map<String, List<SiteMeasurement>> currentSpeedMeasurementsForMeasurementsSites = trn.runTrafficNDWSpeed(ndwIdPattern, startDate, endDate);

        WeatherRetrieverKNMI wrk = new WeatherRetrieverKNMI(sc);
        Map<String, List<String>> weatherObservationsForMeasurementSites = wrk.run(ndwIdPattern, startDateStringKNMI, endDateStringKNMI);

        TrafficWeatherAggregator twa = new TrafficWeatherAggregator();
        twa.getWeatherAndTrafficPerMeasurementSite(currentSpeedMeasurementsForMeasurementsSites, weatherObservationsForMeasurementSites, startDateString, endDateString, true, TrafficWeatherAggregator.ExportFormat.BOS, mCompanionProperties.getProperty("ndw.exportFolder"));

      } else if (cmd.hasOption("export")) {
        String[] arguments = cmd.getOptionValues("export");
        List<Instant> returnDates = parseProcessingArguments(arguments, startDate, endDate, options);
        startDateString = arguments[0];
        endDateString = arguments[1];
        System.out.println("Start date: " + startDateString + ", end date: " + endDateString);
        startDate = returnDates.get(0);
        endDate = returnDates.get(1);
        startDateStringKNMI = formatterWeatherKNMI.format(startDate);
        endDateStringKNMI = formatterWeatherKNMI.format(endDate);
        System.out.println("Start date KNMI: " + startDateStringKNMI + " - end date KNMI: " + endDateStringKNMI + " (from command line)");

        TrafficRetrieverNDW trn = new TrafficRetrieverNDW(sc, mCompanionProperties);
        // trn.runCurrentMeasurements(ftpUrl);
        Map<String, List<SiteMeasurement>> currentSpeedMeasurementsForMeasurementsSites = trn.runTrafficNDWSpeed(ndwIdPattern, startDate, endDate);

        WeatherRetrieverKNMI wrk = new WeatherRetrieverKNMI(sc);
        Map<String, List<String>> weatherObservationsForMeasurementSites = wrk.run(ndwIdPattern, startDateStringKNMI, endDateStringKNMI);

        TrafficWeatherAggregator twa = new TrafficWeatherAggregator();
        twa.getWeatherAndTrafficPerMeasurementSite(currentSpeedMeasurementsForMeasurementsSites, weatherObservationsForMeasurementSites, startDateString, endDateString, false, TrafficWeatherAggregator.ExportFormat.HDF5, mCompanionProperties.getProperty("ndw.hdfFolder"));

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