package no.stcorp.com.companion.weather;

import no.stcorp.com.companion.database.*;
import no.stcorp.com.companion.xml.*;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

import java.io.*;
import java.io.Serializable;

import java.time.*;
import java.time.format.*;

import java.util.*;
import java.util.Map.*;
import java.util.stream.Collectors;

/**
 * Class responsible for obtaining weather data from the Dutch meteorological institute (KNMI)
 */
public class WeatherRetrieverKNMI implements Serializable {
 	private static final long serialVersionUID = 51559559L;

  private static JavaSparkContext mSparkContext;

  /**
   * Constructor
   * @param pSparkContext The Spark context needed for example to download files from the KNMI website
   */
	public WeatherRetrieverKNMI(JavaSparkContext pSparkContext) {
    mSparkContext = pSparkContext;
	}

	/**
	 * Download weather from the KNMI for the traffic measurement sites that match specified ndw id pattern within the specified
	 * start and end date.
	 * @param pNwdIdPattern The pattern the NDW id should match
	 * @param pStartDate Start date in format yyyyMMddHH
	 * @param pEndDate End date in format yyyyMMddHH
	 * @return A map containing all relevant weather observations per NDW measurement site
	 *
	 * HH represents the hour that was just finished (i.e. 15 implies 14.00 - 15.00). To get an entire day typically use
	 * 01 - 24 for the hours
	 */
	public Map<String, List<String>> run(String pNdwIdPattern, String pStartDate, String pEndDate) {
    System.out.println("Downloading weather for ndw id pattern: " + pNdwIdPattern + ", start date: " + pStartDate + ", end date: " + pEndDate);
    Map<String, List<String>> weatherObservationsForMeasurementSite = new HashMap<String, List<String>>();
    String url = "http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi";
    HttpClient client = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost(url);
    String USER_AGENT = "Mozilla/5.0";
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    System.out.println("getWeatherKNMIObservations - start date: " + pStartDate + ", end date: " + pEndDate);
    // Remove the hours from the URL as the KNMI API implies that only these hours for the days are being fetched (e.g 2016010114 - 2016010116 implies from 14 to 16 each day, not including the other hours in between start and end date)
    final String startDateWithoutHours = pStartDate.substring(0, 8);
    final String endDateWithoutHours = pEndDate.substring(0, 8);
    String startDateWithCorrectedHours = startDateWithoutHours + "01";
    String endDateWithCorrectedHours = endDateWithoutHours + "24";
    System.out.println("getWeatherKNMIObservations - start date without hours: " + startDateWithoutHours + ", end date without hours: " + endDateWithoutHours);
    String startHourString = pStartDate.substring(8);
    String endHourString = pEndDate.substring(8);
    int startHourFromArgs = -1;
    int endHourFromArgs = -1;
    try {
      startHourFromArgs = Integer.valueOf(startHourString.trim());
      endHourFromArgs = Integer.valueOf(endHourString.trim());
    } catch (Exception ex) {
      System.err.println("Hour not formattted as integer");
      ex.printStackTrace();
    }
    final int startHour = startHourFromArgs;
    final int endHour = endHourFromArgs;
    System.out.println("getWeatherKNMIObservations - Start hour from args: " + startHour + ", end hour: " + endHour);

    urlParameters.add(new BasicNameValuePair("start", startDateWithCorrectedHours));
    urlParameters.add(new BasicNameValuePair("end", endDateWithCorrectedHours));
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
      JavaRDD<String> weatherData = mSparkContext.textFile(tmpFile.getAbsolutePath()).cache();
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
      JavaRDD<String> weatherObservations = mSparkContext.textFile(tmpFileObservations.getAbsolutePath()).cache();
      System.out.println("Weather data count after parsing: " + weatherObservations.count());

      EntityUtils.consume(response.getEntity()); // To make sure everything is properly released

      // Get the weather and traffic for a traffic measurement point (or more than one). Currently matching a NDW id pattern.
      DatabaseManager dbMgr = DatabaseManager.getInstance();
      Map<String, Integer> weatherStationForMeasurementPoints = dbMgr.getWeatherStationForMeasurementPoints(pNdwIdPattern);
      for (Entry<String, Integer> wsMp : weatherStationForMeasurementPoints.entrySet()) {
        int selectedStationNdwId = wsMp.getValue();
        System.out.println("Measurement point " + wsMp.getKey() + " gets weather from weather station " + selectedStationNdwId + "; getting weather observations for this station");
        List<String> weatherObservationsForStation = weatherObservations.filter(new Function<String, Boolean>() {
          private static final long serialVersionUID = 7L;
          public Boolean call(String s) { 
            // Filter on the station ndw id
            boolean forThisStation = s.trim().startsWith(String.valueOf(selectedStationNdwId));
            if (forThisStation) {
              // Filter on the hour
              String[] woElements = s.split(",");
              if (woElements.length == 25) {
                String dateString = woElements[1].trim();
                String hourString = woElements[2].trim();
                try {
                  int hourObservation = Integer.valueOf(hourString.trim());
                  String timeString = dateString + String.format("%02d", hourObservation);
                  // System.out.println("timeString: " + timeString + ", dateString: " + dateString + ", startDateWithoutHours: " + startDateWithoutHours + ", endDateWithoutHours: " + endDateWithoutHours + ", hourObservation: " + hourObservation);
                  // waitForUserInput();
                  if (dateString.equals(startDateWithoutHours)) {
                    // First day, so only take the relevant hours
                    if (hourObservation < startHour) {
                      forThisStation = false;
                    }
                  } 
                  // Not an else as on the same day this might lead to problems not reaching this condition
                  if (dateString.equals(endDateWithoutHours)) {
                    // Last day, so only take the relevant hours
                    if (hourObservation > endHour) {
                      forThisStation = false;
                    }
                  }
                } catch (Exception ex) {
                  System.err.println("Hour in file not formattted as integer");
                  ex.printStackTrace();
                }
              }
            }
            return forThisStation;
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

	private String readInputStream(InputStream input) throws IOException {
    try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
      return buffer.lines().collect(Collectors.joining("____"));
    }
  }  


	
}