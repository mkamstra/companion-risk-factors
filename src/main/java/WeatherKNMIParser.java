import java.util.*;
import java.util.logging.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.*;

import java.io.*;
import java.sql.*;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

/**
 * Download observed weather on an hourly basis from the KNMI website. The url to be used is:
 * http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi
 * 
 * The parameters to be used are:
 * start=YYYYMMDDHH
 * end=YYYYMMDDHH
 * vars=ALL (all parameters)
 * stns=ALL (all stations)
 */
public class WeatherKNMIParser implements Function<String, String> {

	private final static Logger LOGGER = Logger.getLogger(WeatherKNMIParser.class.getName());

 	private final String USER_AGENT = "Mozilla/5.0";

	private DatabaseManager mDbMgr = null;

 	public WeatherKNMIParser() {
 		try {
			CompanionLogger.setup(WeatherKNMIParser.class.getName());
			LOGGER.setLevel(Level.INFO);
		    mDbMgr = DatabaseManager.getInstance();
		    LOGGER.info("Creating WeatherKNMIParser");
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new RuntimeException("Problem creating log files;" + ex.getMessage());
	    }
 	}

  public String call(String pWeatherObservations) {
    // Parse the XML formatted string using the DOM parser which is good to have all elements loaded in memory, but is known not to be the fastest parser
    List<String> weatherObservations = new ArrayList<String>();
    try {
      LOGGER.info("Starting to parse weather observatiton text file from KNMI:");
      LOGGER.info("Length of text file: " + pWeatherObservations.length());
      List<WeatherStation> weatherStations = new ArrayList<WeatherStation>();
      String[] weatherData = pWeatherObservations.split("____");
	  int index = 0;
	  boolean stationCodes = false;
	  boolean observations = false;
	  String[] observationParameters = new String[]{};
      for (String inputLine : weatherData) {
		index++;
		if (stationCodes) {
			String[] stationLineElements = inputLine.split(" ");
			List<String> stationElements = new ArrayList<String>();
			for (String elt : stationLineElements) {
				if ((elt.length() == 0) || (elt.equals("#")))
					continue;

				stationElements.add(elt);
			}
			if (stationElements.size() < 5) {
				if (stationElements.size() > 0)
					LOGGER.severe("Station info not correct in file: " + inputLine);
			} else {
				LOGGER.finest(inputLine);
				String codeString = stationElements.get(0).trim();
				if (codeString.endsWith(":")) {
					codeString = codeString.substring(0, codeString.length() - 1);
				}
				LOGGER.finest("Code : " + codeString);
				LOGGER.finest("Lon  : " + stationElements.get(1));
				LOGGER.finest("Lat  : " + stationElements.get(2));
				LOGGER.finest("Alt  : " + stationElements.get(3));
				String name = stationElements.get(4);
				for (int i = 5; i < stationElements.size(); i++) {
					name += " " + stationElements.get(i);
				}
				LOGGER.finest("Name : " + name);
				try {
					WeatherStation station = new WeatherStation(Integer.valueOf(codeString), name, Float.valueOf(stationElements.get(2)), Float.valueOf(stationElements.get(1)), Float.valueOf(stationElements.get(3)));
					weatherStations.add(station);
				} catch (Exception ex) {
					LOGGER.severe("Something went wrong trying to create WeatherStation object from a parsed line of data;" + ex.getMessage());
					ex.printStackTrace();
				}
			}
		} else if (observations) {
			if (inputLine.startsWith("#")) {
				continue;
			}
			weatherObservations.add(inputLine);
			//String[] observationElements = inputLine.split(",");
			// for (int i = 0; i < observationParameters.length; i++) {
			// 	System.out.print(observationParameters[i] + ": " + observationElements[i] + " ");
			// }
			// System.out.println();
		} else {
			LOGGER.finest(index + ": " + inputLine);
		}

		if (inputLine.startsWith("# STN      LON(east)   LAT(north)     ALT(m)  NAM")) {
			/** 
			 * Look for the line containing "# # # STN" and the line containing
			 * "# # YYYYMMDD". In between the station codes can be found. 
			 */
			stationCodes = true;
			continue; // Straight away to next iteration
		} else if (inputLine.startsWith("# YYYYMMDD = datum (YYYY=jaar,MM=maand,DD=dag)")) {
			stationCodes = false;
			continue;
		} else if (inputLine.startsWith("# STN,YYYYMMDD,   HH,   DD,   FH,   FF,   FX,    T,  T10,   TD,   SQ,    Q,   DR,   RH,    P,   VV,    N,    U,   WW,   IX,    M,    R,    S,    O,    Y")) {
			observationParameters = inputLine.split(",");
			// System.out.println("Number of elements: " + observationParameters.length);
			// for (int i = 0; i < observationParameters.length; i++) {
			// 	System.out.print(observationParameters[i]);
			// }
			// System.out.println();
			observations = true;
			continue;
		}
      }

      LOGGER.info("Finished parsing weather observations text file from KNMI; now add weather stations to database (if not already there)");

      // Fill database with measurement sites
      int nrOfRowsAdded = mDbMgr.addWeatherStationsToDb(weatherStations);
      LOGGER.info("Finished adding weather stations to database. In total " + nrOfRowsAdded + " weather stations were added to the database");
    //} catch (SQLException ex) {
    //  ex.printStackTrace();
    //  LOGGER.severe("Something went wrong trying to add weater stations to the database; " + ex.getMessage());
    } catch (Exception ex) {
      LOGGER.severe("Something went wrong trying to parse the text file containing weather observations downloaded from the KNMI; " + ex.getMessage());
      ex.printStackTrace();
    }

    String weatherObservationsString = "";
    for (String wo : weatherObservations) {
    	weatherObservationsString += wo + "\n";
    }
    return weatherObservationsString;
  }	

	// // HTTP GET request
	// private void sendGet() throws Exception {

	// 	String url = "http://www.google.com/search?q=mkyong";
		
	// 	URL obj = new URL(url);
	// 	HttpURLConnection con = (HttpURLConnection) obj.openConnection();

	// 	// optional default is GET
	// 	con.setRequestMethod("GET");

	// 	//add request header
	// 	con.setRequestProperty("User-Agent", USER_AGENT);

	// 	int responseCode = con.getResponseCode();
	// 	System.out.println("\nSending 'GET' request to URL : " + url);
	// 	System.out.println("Response Code : " + responseCode);

	// 	BufferedReader in = new BufferedReader(
	// 	        new InputStreamReader(con.getInputStream()));
	// 	String inputLine;
	// 	StringBuffer response = new StringBuffer();

	// 	while ((inputLine = in.readLine()) != null) {
	// 		response.append(inputLine);
	// 	}
	// 	in.close();

	// 	//print result
	// 	System.out.println(response.toString());

	// }
	
	// HTTP POST request
	/**
	 * @param startYear
	 * @param startMonth Month from 1 to 12
	 * @param startDay Day from 1 to 31
	 * @param startHour Hour from 1 to 24
	 * @param endYear
	 * @param endMonth Month from 1 to 12
	 * @param endDay Day from 1 to 31
	 * @param endHour Hour from 1 to 24
	 */
	private void downloadObservations(int startYear, int startMonth, int startDay, int startHour, int endYear, int endMonth, int endDay, int endHour) throws Exception {

		long startTime = System.currentTimeMillis();
		String url = "http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi";
		URL obj = new URL(url);
		//HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		//add reuqest header
		con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

		String urlParameters = "start=" + startYear + String.format("%02d", startMonth) + String.format("%02d", startDay) + String.format("%02d", startHour) + 
			"&end=" + endYear + String.format("%02d", endMonth) + String.format("%02d", endDay) + String.format("%02d", endHour) + "&vars=ALL&stns=ALL";
		
		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'POST' request to URL : " + url);
		System.out.println("Post parameters : " + urlParameters);
		System.out.println("Response Code : " + responseCode);

		long startParsingTime = System.currentTimeMillis();

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		//StringBuffer response = new StringBuffer();

		int index = 0;
		boolean stationCodes = false;
		boolean observations = false;
		String[] observationParameters = new String[]{};
		long stationsFinishedTime = System.currentTimeMillis();
		while ((inputLine = in.readLine()) != null) {
			index++;
			//response.append(inputLine);
			if (stationCodes) {
				String[] stationLineElements = inputLine.split(" ");
				List<String> stationElements = new ArrayList<String>();
				for (String elt : stationLineElements) {
					if ((elt.length() == 0) || (elt.equals("#")))
						continue;

					stationElements.add(elt);
				}
				if (stationElements.size() < 5) {
					if (stationElements.size() > 0)
						System.out.println("Station info not correct in file: " + inputLine);
				} else {
					System.out.println("Code : " + stationElements.get(0));
					System.out.println("Lon  : " + stationElements.get(1));
					System.out.println("Lat  : " + stationElements.get(2));
					System.out.println("Alt  : " + stationElements.get(3));
					String name = stationElements.get(4);
					for (int i = 5; i < stationElements.size(); i++) {
						name += " " + stationElements.get(i);
					}
					System.out.println("Name : " + name);
				}
			} else if (observations) {
				if (inputLine.startsWith("#")) {
					continue;
				}
				String[] observationElements = inputLine.split(",");
				for (int i = 0; i < observationParameters.length; i++) {
					System.out.print(observationParameters[i] + ": " + observationElements[i] + " ");
				}
				System.out.println();
			} else {
				System.out.println(index + ": " + inputLine);
			}

			if (inputLine.startsWith("# STN      LON(east)   LAT(north)     ALT(m)  NAM")) {
				/** 
				 * Look for the line containing "# # # STN" and the line containing
				 * "# # YYYYMMDD". In between the station codes can be found. 
				 */
				stationCodes = true;
				continue; // Straight away to next iteration
			} else if (inputLine.startsWith("# YYYYMMDD = datum (YYYY=jaar,MM=maand,DD=dag)")) {
				stationCodes = false;
				stationsFinishedTime = System.currentTimeMillis();
				continue;
			} else if (inputLine.startsWith("# STN,YYYYMMDD,   HH,   DD,   FH,   FF,   FX,    T,  T10,   TD,   SQ,    Q,   DR,   RH,    P,   VV,    N,    U,   WW,   IX,    M,    R,    S,    O,    Y")) {
				observationParameters = inputLine.split(",");
				System.out.println("Number of elements: " + observationParameters.length);
				for (int i = 0; i < observationParameters.length; i++) {
					System.out.print(observationParameters[i]);
				}
				System.out.println();
				observations = true;
				continue;
			}

		}
		in.close();
		
		long observationsFinishedTime = System.currentTimeMillis();
		System.out.println("Total time used     : " + (observationsFinishedTime - startTime));
		System.out.println("  Download time     : " + (startParsingTime - startTime));
		System.out.println("  Stations time     : " + (stationsFinishedTime - startParsingTime));
		System.out.println("  Observations time : " + (observationsFinishedTime - stationsFinishedTime));
		//print result
		//System.out.println(response.toString());

	}

}