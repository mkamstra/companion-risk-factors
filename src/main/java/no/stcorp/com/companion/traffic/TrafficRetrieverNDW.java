package no.stcorp.com.companion.traffic;

import no.stcorp.com.companion.database.*;
import no.stcorp.com.companion.util.*;
import no.stcorp.com.companion.xml.*;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkFiles;

import java.io.*;

import java.nio.file.*;

import java.time.*;
import java.time.format.*;
import java.time.temporal.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.Map.*;
import java.util.stream.Collectors;

public class TrafficRetrieverNDW implements Serializable {
 	private static final long serialVersionUID = 51559559L;

  private static JavaSparkContext mSparkContext;
	
  /**
   * Constructor
   * @param pSparkContext The Spark context needed for example to download files from the NDW website
   */
	public TrafficRetrieverNDW(JavaSparkContext pSparkContext) {
		mSparkContext = pSparkContext;
	}

  /**
   * Download traffic current measurements from NDW. Is used for checking if the measurement sites are all
   * present in the database. If not they will be added.
   * @param pFtpUrl The FTP address to download from exluding the folder of the day
   * @param pDate The date to download for 
   */
  public void runCurrentMeasurements(String pFtpUrl, Instant pDate) {
    // Current measurements for getting the measurement sites
    // Download data from NDW
    System.out.println("Trying to download gzip file containing measurements from NDW");
    String rootDir = SparkFiles.getRootDirectory(); // Get the location where the files added with addFile are downloaded
    System.out.println("Gzipped files will be downloaded to: " + rootDir);

    System.out.println("Downloading current measurements containing the measurement locations");
    DateTimeFormatter formatterAutomaticDownload = DateTimeFormatter.ofPattern("yyyy_MM_dd").withZone(ZoneId.systemDefault());
    String dayFolderName = "//" + formatterAutomaticDownload.format(pDate) + "//";
    // For now take a file close to 08.30 in the morning as this is often a busy time and a file with considerable data is expected
    FTPClient ftpClient = new FTPClient();
    List<String> relevantFiles = new ArrayList<String>();
    String relevantFile = "";
    try {
      boolean useLocalFile = true;
      List<String> allFiles = new ArrayList<String>();
      if (!useLocalFile) { // temp to be able to read the data
        ftpClient.connect("192.168.1.33");
        ftpClient.enterLocalPassiveMode();
        ftpClient.login("companion", "1d1ada");
        System.out.println("Working directory FTP server (1): " + ftpClient.printWorkingDirectory());
        ftpClient.changeWorkingDirectory("//Projects//companion//downloadedData//NDW//");
        System.out.println("Working directory FTP server (2): " + ftpClient.printWorkingDirectory());
        boolean directoryExists = ftpClient.changeWorkingDirectory("//Projects//companion//downloadedData//NDW//" + dayFolderName);
        System.out.println("Working directory FTP server (3a): " + ftpClient.printWorkingDirectory());
        // Check if folder exists
        if (!directoryExists) {
          System.out.println("Directory //Projects//companion//downloadedData//NDW//" + dayFolderName + " does not exist");
          // No automatic folder, so return
          return;
        }
        FTPFile[] allFtpFiles = ftpClient.listFiles();
        ftpClient.logout();
        System.out.println("Files in folder: " + allFtpFiles.length);
        for (FTPFile ftpFile : allFtpFiles) {
          // System.out.println(ftpFile.getName());
          allFiles.add(ftpFile.getName());
        }
        System.out.println("Directory exists: " + directoryExists);
      } else {
        Path localDir = Paths.get("//usr//local//data//ndw//");
        System.out.println("Listing files in " + localDir.toString());
        DirectoryStream<Path> stream = Files.newDirectoryStream(localDir);
        Iterator<Path> iter = stream.iterator();
        while (iter.hasNext()) {
          Path path = iter.next();
          String fileName = path.toString();
          System.out.println(fileName);
          allFiles.add(fileName);
        }

        //allFiles = Arrays.deepToString(localDir.listFiles((File f) -> (f.getName().endsWith(".xml.gz") && f.getName().startsWith("measurement_current"))));
      }
      //  Filter by measurement_current in name
      List<String> filesForDay = allFiles.stream().filter(s -> s.contains("measurement_current")).collect(Collectors.toList());
      int hour = 0;
      while (relevantFile.length() == 0 && hour < 24) {
        // Filter by hour
        hour++;
        String hourFilterBaseString = "measurement_current_" + formatterAutomaticDownload.format(pDate) + "_" + String.format("%02d", hour);
        List<String> filesForHour = filesForDay.stream().filter(s -> s.contains(hourFilterBaseString)).collect(Collectors.toList());
        System.out.println("Hour: " + hour + ", number of relevant files after filtering on hour: " + filesForHour.size() + " (" + hourFilterBaseString + ")");
        int minute = 30;
        final String minuteFilterBaseString = hourFilterBaseString + "_" + String.format("%02d", minute);
        List<String> filesForMinute = filesForHour.stream().filter(s -> s.contains(minuteFilterBaseString)).collect(Collectors.toList());
        System.out.println("Hour: " + hour + ", minute: " + minute + ", number of relevant files after filtering on hour and minute: " + filesForMinute.size() + " (" + minuteFilterBaseString + ")");
        
        // In case not available for that minute, then change minute until found
        while (filesForMinute.size() == 0 && minute != 29) {
          minute += 1;
          if (minute == 60) 
            minute = 0;

          final String loopMinuteFilterBaseString = hourFilterBaseString + "_" + String.format("%02d", minute);
          filesForMinute = filesForHour.stream().filter(s -> s.contains(loopMinuteFilterBaseString)).collect(Collectors.toList());
          System.out.println("Hour: " + hour + ", minute: " + minute + ", number of relevant files after filtering on hour and minunte: " + filesForMinute.size());
        } 
        if (filesForMinute.size() > 0) {
          String fileNamePlusPath = filesForMinute.get(0);
          relevantFile = FilenameUtils.getName(fileNamePlusPath); // Base file name
          dayFolderName = fileNamePlusPath.substring(0, fileNamePlusPath.length() - relevantFile.length()); // Local folder
        }
      }

      if (relevantFile.length() == 0)
        return;

      String measurementZipUrl = pFtpUrl +  dayFolderName + relevantFile;
      if (useLocalFile)
        measurementZipUrl = dayFolderName + relevantFile; // No need to provide ftp as the file is local

      System.out.println("Ftp file path for measurement file: " + measurementZipUrl);
      mSparkContext.addFile(measurementZipUrl);
      String measurementFilePath = SparkFiles.get(relevantFile);
      System.out.println("Measurement file path: " + measurementFilePath);

      JavaRDD<String> gzData = mSparkContext.textFile(measurementFilePath).cache(); // textFile should decompress gzip automatically
      //System.out.println("Output: " + gzData.toString());

      // try {
      //   System.out.println("Putting app to sleep for 10 seconds again");
      //   Thread.sleep(10000);
      // } catch (InterruptedException ex) {
      //   System.out.println("Something went wrong putting the app to sleep for 100 seconds again");
      //   ex.printStackTrace();
      //   Thread.currentThread().interrupt();
      // }

      Path measurementFileAsPath = Paths.get(measurementFilePath);
      Path localDir = measurementFileAsPath.getParent();
      System.out.println("Listing files in " + localDir.toString());
      DirectoryStream<Path> stream = Files.newDirectoryStream(localDir);
      Iterator<Path> iter = stream.iterator();
      while (iter.hasNext()) {
        Path path = iter.next();
        String fileName = path.toString();
        System.out.println(fileName);
      }

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
      } catch (Exception ex) {
        System.out.println("Problem reading the current measurements file");
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
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally{
      try {
        ftpClient.disconnect();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }


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
   * This utility simply wraps a functional
   * interface that throws a checked exception
   * into a Java 8 Consumer
   */
  private static <T> Consumer<T>
  unchecked(CheckedConsumer<T> consumer) {
    return t -> {
      try {
        consumer.accept(t);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @FunctionalInterface
  private interface CheckedConsumer<T> {
    void accept(T t) throws Exception;
  }

  /**
   * Get the speed traffic data from the NDW site. To avoid processing to much traffic data only the traffic files of every
   * fifth minute is used
   * @param pFtpUrl The FTP address to download from 
   * @param pNdwIdPattern The pattern the NDW id should match
	 * @param pStartDate Start date in format yyyyMMddHH
	 * @param pEndDate End date in format yyyyMMddHH
   * @return a map of traffic speed measurements per measurement site
   */
  public Map<String, List<SiteMeasurement>> runTrafficNDWSpeed(String pFtpUrl, String pNdwIdPattern, Instant pStartDate, Instant pEndDate) {
    Map<String, List<SiteMeasurement>> measurementsPerSite = new HashMap<String, List<SiteMeasurement>>();

    System.out.println("Downloading traffic speed");
    List<String> relevantFiles = getRelevantTrafficSpeedFiles(pStartDate, pEndDate);
    // System.out.println("Relevant files: ");
    // for (String trafficFileName : relevantFiles) {
    //   System.out.println(trafficFileName);
    // }
    // TODO MK: Remove the following lines again as they are just there to reduce the processing time while testing
    relevantFiles = relevantFiles.stream().filter(filename -> filename.contains("00_") || filename.contains("10_") || filename.contains("20_") 
    	|| filename.contains("30_") || filename.contains("40_") || filename.contains("50_") || filename.contains("05_") || filename.contains("15_") 
    	|| filename.contains("25_") || filename.contains("35_") || filename.contains("45_") || filename.contains("55_")).collect(Collectors.toList());
    //relevantFiles = relevantFiles.subList(0,2);
    System.out.println("Relevant files: ");
    for (String trafficFileName : relevantFiles) {
      System.out.println(trafficFileName);
    }
    // waitForUserInput();
    // try {
    //   System.out.println("Putting app to sleep for 100 seconds again");
    //   Thread.sleep(100000);
    // } catch (InterruptedException ex) {
    //   System.out.println("Something went wrong putting the app to sleep for 100 seconds again");
    //   ex.printStackTrace();
    //   Thread.currentThread().interrupt();
    // }

    Utils.printFileDetailsForFolder(Paths.get("/tmp"));
    for (String trafficFileName : relevantFiles) {
      try {
        //String trafficFileName = "trafficspeed_2016_01_08_16_32_38_230.xml.gz";
        String trafficZipUrl = pFtpUrl + trafficFileName;
        System.out.println("Traffic zip URL: " + trafficZipUrl);
        mSparkContext.addFile(trafficZipUrl);
        String fileNameWithoutDay = Paths.get(trafficFileName).getFileName().toString();
        String trafficFilePath = SparkFiles.get(fileNameWithoutDay);
        System.out.println("Traffic file path: " + trafficFilePath);

        JavaRDD<String> gzData = mSparkContext.textFile(trafficFilePath).cache(); // textFile should decompress gzip automatically
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
          JavaRDD<SiteMeasurement> measurementsDistributed = mSparkContext.parallelize(measurements); // Parallelizing the existing collection
          System.out.println("Number of speed measurements: " + measurementsDistributed.count());
          DatabaseManager dbMgr = DatabaseManager.getInstance();
          List<String> ndwIds = dbMgr.getNdwIdsFromNdwIdPattern(pNdwIdPattern);
          // Now determine the measurements per station
          for (String ndwId : ndwIds) {
            // Filter based on ndw id and then collect into a list
            List<SiteMeasurement> measurementsForSite = measurementsDistributed.filter(ms -> ms.getMeasurementSiteReference().equalsIgnoreCase(ndwId)).collect();
            if (measurementsPerSite.containsKey(ndwId)) {
              // measurementsForSite has been created in such a way that it is not modifyable (like with Arrays.asList()). Therefore
              // first initialize list and then add elements.
              List<SiteMeasurement> existingMeasurements = new ArrayList<SiteMeasurement>();
              existingMeasurements.addAll(measurementsPerSite.get(ndwId));
              existingMeasurements.addAll(measurementsForSite);
              measurementsPerSite.put(ndwId, existingMeasurements);
            } else {
              measurementsPerSite.put(ndwId, measurementsForSite);
            }
          }
        }
      } catch (Exception ex) {
        System.err.println("Something went wrong reading and parsing the file " + trafficFileName);
        ex.printStackTrace();
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
    }
    return measurementsPerSite;
  }

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
	 * @param pStartDate Start date in format yyyyMMddHH
	 * @param pEndDate End date in format yyyyMMddHH
	 * @return  A list of files satisfying the date conditions. Note that the filenames include the folder name of the day
   */
  private List<String> getRelevantTrafficSpeedFiles(Instant pStartDate, Instant pEndDate) {
    // Get the day as the traffic data is organised by folders
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    DateTimeFormatter formatterAutomaticDownload = DateTimeFormatter.ofPattern("yyyy_MM_dd").withZone(ZoneId.systemDefault());
    DateTimeFormatter formatterManualDownload = DateTimeFormatter.ofPattern("dd-MM-yyyy").withZone(ZoneId.systemDefault());
    DateTimeFormatter formatterHours = DateTimeFormatter.ofPattern("HH").withZone(ZoneId.systemDefault());
    String hoursStartString = formatterHours.format(pStartDate);
    int hoursStart = 0;
    String hoursEndString = formatterHours.format(pEndDate);
    int hoursEnd = 0;
    try {
      hoursStart = Integer.valueOf(hoursStartString);
      hoursEnd = Integer.valueOf(hoursEndString);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    System.out.println("Hours start: " + hoursStart + ", hours end: " + hoursEnd);
    List<Instant> relevantDays = new ArrayList<Instant>();
    relevantDays.add(pStartDate);
    // Days between start and end date
    long daysBetween = ChronoUnit.DAYS.between(pStartDate, pEndDate);
    if (daysBetween == 0 && hoursEnd == 0)
        hoursEnd = 24;

    for (long day = 1; day <= daysBetween; day++) {
      Instant extraDate = pStartDate.plus(day, ChronoUnit.DAYS);
      relevantDays.add(extraDate);
    }
    System.out.println("Days in between: " + daysBetween);
    boolean firstDay = true;
    boolean lastDay = false;
    FTPClient ftpClient = new FTPClient();
    List<String> relevantFiles = new ArrayList<String>();
    try {
      ftpClient.connect("192.168.1.33");
      ftpClient.enterLocalPassiveMode();
      ftpClient.login("companion", "1d1ada");
      System.out.println("Working directory FTP server (1): " + ftpClient.printWorkingDirectory());
      for (int i = 0; i < relevantDays.size(); i++) {
        ftpClient.changeWorkingDirectory("//Projects//companion//downloadedData//NDW//");
        System.out.println("Working directory FTP server (2): " + ftpClient.printWorkingDirectory());
        if (i == relevantDays.size() - 1) {
          lastDay = true;
        }
        Instant day = relevantDays.get(i);
        String dayFolderName = formatterAutomaticDownload.format(day);
        boolean automaticFolder = true;
        boolean directoryExists = ftpClient.changeWorkingDirectory(dayFolderName);
        System.out.println("Working directory FTP server (3a): " + ftpClient.printWorkingDirectory());
        // Check if folder exists
        if (!directoryExists) {
          // Check if manual folder exists
          dayFolderName = formatterManualDownload.format(day);
          directoryExists = ftpClient.changeWorkingDirectory(dayFolderName);
          System.out.println("Working directory FTP server (3b): " + ftpClient.printWorkingDirectory());
          if (directoryExists) {
            automaticFolder = false;
          } else {
            System.err.println("No folder for date " + dayFolderName + " exists");
            continue;
          }
        }
        System.out.println("First day: " + firstDay + ", last day: " + lastDay);
        int startHour = 0;
        if (firstDay) {
          startHour = hoursStart;
        }
        int endHour = 24;
        if (lastDay) {
          endHour = hoursEnd;
        }
        System.out.println("Working directory FTP server (4): " + ftpClient.printWorkingDirectory());
        FTPFile[] allFtpFiles = ftpClient.listFiles();
        System.out.println("Files in folder: " + allFtpFiles.length);
        List<String> allFiles = new ArrayList<String>();
        for (FTPFile ftpFile : allFtpFiles) {
          // System.out.println(ftpFile.getName());
          allFiles.add(ftpFile.getName());
        }
        System.out.println("Directory exists: " + directoryExists);
        if (directoryExists) {
          if (automaticFolder) {
            System.out.println("Automatic folder");
            //  Filter by trafficspeed in name
            List<String> filesForDay = allFiles.stream().filter(s -> s.contains("trafficspeed")).collect(Collectors.toList());
            // Filter by hour
            for (int hour = startHour; hour < endHour; hour++) {
              String hourFilterBaseString = "trafficspeed_" + dayFolderName + "_" + String.format("%02d", hour);
              List<String> filesForHour = filesForDay.stream().filter(s -> s.contains(hourFilterBaseString)).collect(Collectors.toList());
              System.out.println("Hour: " + hour + ", number of relevant files: " + filesForHour.size());
              for (String fileForHour : filesForHour) {
                relevantFiles.add("//" + dayFolderName + "//" + fileForHour);
              }
            }
          } else {
            System.out.println("Manual folder");
            //  Filter by trafficspeed in name
            List<String> filesForDay = allFiles.stream().filter(s -> s.contains("Trafficspeed")).collect(Collectors.toList());
            // Filter by hour
            for (int hour = startHour; hour < endHour; hour++) {
              String hourString = String.format("%02d", hour);
              List<String> filesForHour = filesForDay.stream().filter(s -> s.startsWith(hourString)).collect(Collectors.toList());
              System.out.println("Hour: " + hour + ", number of relevant files: " + filesForHour.size());
              for (String fileForHour : filesForHour) {
                relevantFiles.add("//" + dayFolderName + "//" + fileForHour);
              }
            }
          }
        }
        firstDay = false;
      }
      ftpClient.logout();
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally{
      try {
        ftpClient.disconnect();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    return relevantFiles;
  }

  /**
   * @param pSiteMeasurements The map of site measurements per measurement id (NDW id)
   * Print pSiteMeasurements to standard output
   */
  public void printSiteMeasurementsPerSite(Map<String, List<SiteMeasurement>> pSiteMeasurements) {
    for (Entry<String, List<SiteMeasurement>> siteMeasurementEntry : pSiteMeasurements.entrySet()) {
      String ndwId = siteMeasurementEntry.getKey();
      List<SiteMeasurement> sms = siteMeasurementEntry.getValue();
      System.err.println("=================================================");
      System.out.println("Measurement site: " + ndwId);
      System.out.println("  Traffic:");
      for (SiteMeasurement sm : sms) {
        System.out.println("    " + sm);
      }
    }
  }
}