import java.io.Serializable;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Map.*;
import java.util.logging.*;

public class DatabaseManager implements Serializable {
  private final static Logger LOGGER = Logger.getLogger(DatabaseManager.class.getName());
  private static DatabaseManager mInstance = null;
  private static Connection mConnection = null;

  private DatabaseManager() {
  	// Intentionally private to ensure singleton pattern
    try {
      CompanionLogger.setup(DatabaseManager.class.getName());
      LOGGER.setLevel(Level.FINEST);
    } catch (IOException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem creating log files;" + ex.getMessage());
    }
  }

  public static DatabaseManager getInstance() {
  	if (mInstance == null) {
  	  mInstance = new DatabaseManager();
  	}
  	return mInstance;
  }

  private Connection getConnection() throws RuntimeException {
    if (mConnection == null) {
      setupDatabaseConnection("snt", "snt");
    }
    return mConnection;
  }

  private void setupDatabaseConnection(String user, String pw) throws RuntimeException {
    try {
      Class.forName("org.postgresql.Driver");
      //String url = "jdbc:postgresql://localhost/companion";
      //Properties props = new Properties();
      //props.setProperty("user","snt");
      //props.setProperty("password","snt");
      //props.setProperty("ssl","true");
      //Connection conn = DriverManager.getConnection(url, props);
      /**
        * Don't user certificate for now as software is not operational. In case needed use 
        * --driver-java-options "-Djavax.net.ssl.trustStore=mystore -Djavax.net.ssl.trustStorePassword=mypassword" 
        * to set the certificate settings. See https://jdbc.postgresql.org/documentation/94/ssl-client.html and 
        * http://stackoverflow.com/questions/28840438/how-to-override-sparks-log4j-properties-per-driver for 
        * some more explanation on the topic
        */
      String url = "jdbc:postgresql://localhost/companion?user=" + user + "&password=" + pw + "&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
      mConnection = DriverManager.getConnection(url);
    } catch (ClassNotFoundException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem loading the Postgres JDBC driver;" + ex.getMessage());
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem connecting to or manipulating COMPANION database;" + ex.getMessage());
    }
  }

  private void closeConnection() throws RuntimeException {
    try {
      if (mConnection != null) {
        mConnection.close();
        mConnection = null;
      }
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem closing connection to COMPANION database");
    }
  }


  /**
   * A map containing the different ndw types as they are encoded in the database. To insert measurements 
   * sites into the database the integer code is needed instead of the string. 
   */
  public Map<String, Integer> getNdwTypes() {
    Map<String, Integer> ndwTypes = new HashMap<String, Integer>();
	try {
      getConnection();
      Statement st = mConnection.createStatement();
      ResultSet rs = st.executeQuery("SELECT * FROM measurementsitetype");
      while (rs.next())
      {
        int id = rs.getInt("id");
        String ndwType = rs.getString("ndwtype");
        ndwTypes.put(ndwType, id);
      } 
      rs.close();
      st.close(); 
      closeConnection();
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem connecting to COMPANION database;" + ex.getMessage());
    }
    return ndwTypes;
  }

  public Map<String, Integer> getAllMeasurementSiteIdsFromDb() throws RuntimeException {
    Map<String, Integer> measurementSiteIds = new HashMap<String, Integer>();
  	try {
      getConnection();
      Statement st = mConnection.createStatement();
      String allMeasurementsSql = "SELECT ndwid, id from measurementsite";
      ResultSet rs = st.executeQuery(allMeasurementsSql);
      while (rs.next()) {
        String ndwId = rs.getString(1);      
        Integer dbId = rs.getInt(2);
        measurementSiteIds.put(ndwId, dbId);
      }
      rs.close();
      st.close();
      closeConnection();
  	} catch (SQLException ex) {
  	  ex.printStackTrace();
  	  throw new RuntimeException("Problem getting all measurement site ids from the database;" + ex.getMessage());
  	}
    return measurementSiteIds;
  }

  public int addMeasurementSitesToDb(List<MeasurementSite> measurementSites) {
  	int nrOfRowsAdded = 0;
  	getConnection();
    List<MeasurementSite> addedMeasurementSites = new ArrayList<MeasurementSite>();
    for (MeasurementSite ms : measurementSites) {
      int addedRows = addMeasurementSiteToDb(ms);
      if (addedRows > 0)
        addedMeasurementSites.add(ms);

      nrOfRowsAdded += addedRows;
    }
    // Link the added measurement sites to weather stations
    for (MeasurementSite ms : addedMeasurementSites) {
      linkMeasurementSiteWithClosestWeatherStation(ms);
    }
    closeConnection();
    return nrOfRowsAdded;
  }

  private int addMeasurementSiteToDb(MeasurementSite ms) throws RuntimeException {
    int nrOfRowsAdded = 0;
    try {
      if (Float.isNaN(ms.getLatitude()) || Float.isNaN(ms.getLongitude())) {
        throw new RuntimeException("Problem adding measurement site to the database as the latitude and longitude of the measurement site have not been filled properly; lat = " + ms.getLatitude() + ", lon = " + ms.getLongitude());
      }
	    getConnection();
	    String selectSql = "SELECT * FROM measurementsite where ndwid='" + ms.getNdwid() + "'";
	    LOGGER.finest("SQL statement to get number of records: " + selectSql);
	    Statement st = mConnection.createStatement();
	    // Check first if measurement site already exists in db
	    ResultSet rs = st.executeQuery(selectSql);
	    if (!rs.next()) {
	      // Does not exist, so add to database
	      String selectMaxSql = "SELECT max(id) FROM measurementsite";
	      LOGGER.finest("SQL statement to get max id: " + selectMaxSql);
	      // Check first if measurement site already exists in db
	      rs = st.executeQuery(selectMaxSql);
	      int maxId = 1;
	      while (rs.next()) {
	        int maxDbId = rs.getInt(1);
	        LOGGER.finest("Max id in measurementsite table: " + maxDbId);
	        maxId = maxDbId + 1;
	        break;
	      }
	      String carriageWay1 = "NULL";
	      if (ms.getCarriageway1() != null) {
	        carriageWay1 = "'" + ms.getCarriageway1() + "'";
	      }
	      String carriageWay2 = "NULL";
	      if (ms.getCarriageway2() != null) {
	        carriageWay2 = "'" + ms.getCarriageway2() + "'";
	      }
	      String location2 = "NULL";
	      if (ms.getLocation2() != null) {
	        location2 = "'" + ms.getLocation2() + "'";
	      }
	      String ndwId = ms.getNdwid();
	      ndwId = ndwId.replaceAll("'", "_");
	      String name = ms.getName();
	      name = name.replaceAll("'", "_");
	      String insertSql = "INSERT INTO measurementsite VALUES(" + maxId +  ", '" + ndwId + "', '" + name + "', " + ms.getNdwtype() + ", ST_GeomFromText('POINT(" + ms.getLatitude() + " " + ms.getLongitude() + ")', 4326), '" + ms.getLocation1() + "', " + 
	        carriageWay1 + ", " + ms.getLengthaffected1() + ", "  + location2 + ", " + carriageWay2 + ", " + ms.getLengthaffected2() + ")";
	      LOGGER.finest(insertSql);
	      int rowsAdded = st.executeUpdate(insertSql);
	      LOGGER.finest("Rows added: " + rowsAdded);
	      nrOfRowsAdded = rowsAdded;
	    } else {
	      LOGGER.finest("Row with NDW id " + ms.getNdwid() + " exists already");
	    }
	    rs.close();
	    st.close();
  	} catch (SQLException ex) {
  		ex.printStackTrace();
  		throw new RuntimeException("Problem adding measurement site to the database; " + ex.getMessage());
    }
    return nrOfRowsAdded;
  }

  public int addWeatherStationsToDb(List<WeatherStation> weatherStations) {
  int nrOfRowsAdded = 0;
  getConnection();
    for (WeatherStation ws : weatherStations) {
      nrOfRowsAdded += addWeatherStationToDb(ws);
    }
    closeConnection();
    return nrOfRowsAdded;
  }

  private int addWeatherStationToDb(WeatherStation ws) throws RuntimeException {
    int nrOfRowsAdded = 0;
    try {
      getConnection();
      String selectSql = "SELECT * FROM weatherstation where knmiid='" + ws.getKnmiId() + "'";
      LOGGER.finest("SQL statement to get number of records: " + selectSql);
      Statement st = mConnection.createStatement();
      // Check first if measurement site already exists in db
      ResultSet rs = st.executeQuery(selectSql);
      if (!rs.next()) {
        // Does not exist, so add to database
        String selectMaxSql = "SELECT max(id) FROM weatherstation";
        LOGGER.finest("SQL statement to get max id: " + selectMaxSql);
        // Check first if measurement site already exists in db
        rs = st.executeQuery(selectMaxSql);
        int maxId = 1;
        while (rs.next()) {
          int maxDbId = rs.getInt(1);
          LOGGER.finest("Max id in weatherstation table: " + maxDbId);
          maxId = maxDbId + 1;
          break;
        }
        String name = ws.getName();
        name = name.replaceAll("'", "_");
        String insertSql = "INSERT INTO weatherstation VALUES(" + maxId +  ", '" + ws.getKnmiId() + "', '" + name + "', ST_GeomFromText('POINT(" + ws.getLatitude() + " " + ws.getLongitude() + ")', 4326), " + ws.getAltitude() + ")";
        LOGGER.finest(insertSql);
        int rowsAdded = st.executeUpdate(insertSql);
        LOGGER.finest("Rows added: " + rowsAdded);
        nrOfRowsAdded = rowsAdded;
      } else {
        LOGGER.finest("Row with KNMI id " + ws.getKnmiId() + " exists already");
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem adding weather station to the database; " + ex.getMessage());
    }
    return nrOfRowsAdded;
  }

  public int linkAllMeasurementSitesWithClosestWeatherStation() throws RuntimeException {
    int nrOfRowsAdded = 0;
    try {
      getConnection();
      Statement st = mConnection.createStatement();
      String selectSql = "select measurementsite.id as msid, (select weatherstation.id as wsid from weatherstation order by measurementsite.location <-> weatherstation.location limit 1) from measurementsite;";
      LOGGER.finest("SQL statement to match measurement sites with their closest weather station: " + selectSql);
      ResultSet rs = st.executeQuery(selectSql);
      Map<Integer, Integer> msAndWsIdsToBeAdded = new HashMap<Integer, Integer>();
      while (rs.next()) {
        int msid = rs.getInt("msid");
        int wsid = rs.getInt("wsid");
        msAndWsIdsToBeAdded.put(msid, wsid);
      }
      for (Entry<Integer, Integer> entry : msAndWsIdsToBeAdded.entrySet()) {
        int msid = entry.getKey();
        int wsid = entry.getValue();
        try {
          String insertSql = "insert into measurementsite_weatherstation_link values(" + msid + ", " + wsid + ");";
          LOGGER.finest(insertSql);
          int rowsAdded = st.executeUpdate(insertSql);
          LOGGER.finest("Rows added: " + rowsAdded);
          nrOfRowsAdded += rowsAdded;
        } catch (SQLException ex) {
          LOGGER.severe("SQL State: " + ex.getSQLState());
          if (ex.getSQLState().equalsIgnoreCase("23505")) { // Unique violation, see Postgres error codes
            LOGGER.severe("Duplicate key found in measurementsite_weatherstation_link (msid, wsid): (" + msid + ", " + wsid + ")");
          } else {
            throw new SQLException("Problem adding combination of measurement site id and weather station id to link table; " + ex.getMessage());
          }
        }
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem adding weather station to the database; " + ex.getMessage());
    }
    return nrOfRowsAdded;
  }

  public int linkMeasurementSiteWithClosestWeatherStation(MeasurementSite ms) throws RuntimeException {
    int nrOfRowsAdded = 0;
    try {
      getConnection();
      Statement st = mConnection.createStatement();
      String selectSql = "select measurementsite.id as msid, weatherstation.id as wsid from measurementsite,weatherstation where measurementside.ndwid = '" + ms.getNdwid() + "' order by measurementsite.location <-> weatherstation.location limit 1;";
      LOGGER.finest("SQL statement to match measurement site with its closest weather station: " + selectSql);
      ResultSet rs = st.executeQuery(selectSql);
      while (rs.next()) {
        int msid = rs.getInt("msid");
        int wsid = rs.getInt("wsid");
        try {
          String insertSql = "insert into measurementsite_weatherstation_link values(" + msid + ", " + wsid + ");";
          LOGGER.finest(insertSql);
          int rowsAdded = st.executeUpdate(insertSql);
          LOGGER.finest("Rows added: " + rowsAdded);
          nrOfRowsAdded += rowsAdded;
        } catch (SQLException ex) {
          LOGGER.severe("SQL State: " + ex.getSQLState());
          if (ex.getSQLState().equalsIgnoreCase("23505")) { // Unique violation, see Postgres error codes
            LOGGER.severe("Duplicate key found in measurementsite_weatherstation_link (msid, wsid): (" + msid + ", " + wsid + ")");
          } else {
            throw new SQLException("Problem adding combination of measurement site id and weather station id to link table; " + ex.getMessage());
          }
        }
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem adding weather station to the database; " + ex.getMessage());
    }
    return nrOfRowsAdded;
  }

  public List<WeatherStation> getAllWeatherStations() throws RuntimeException {
    List<WeatherStation> wsList = new ArrayList();
    try {
      getConnection();
      Statement st = mConnection.createStatement();
      String allWeatherStationsSql = "SELECT knmiid,name,st_x(location) as lat,st_y(location) as lon,altitude from weatherstation;";
      ResultSet rs = st.executeQuery(allWeatherStationsSql);
      while (rs.next()) {
        int knmiid = rs.getInt("knmiid");
        String name = rs.getString("name");
        float lat = rs.getFloat("lat");
        float lon = rs.getFloat("lon");
        float altitude = rs.getFloat("altitude");
        WeatherStation ws = new WeatherStation(knmiid, name, lat, lon, altitude);
        wsList.add(ws);
      }
      rs.close();
      st.close();
      closeConnection();
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem getting all weather stations from the database;" + ex.getMessage());
    }
    return wsList;
  }

  public List<MeasurementSite> getAllMeasurementSites() throws RuntimeException {
    List<MeasurementSite> msList = new ArrayList();
    try {
      getConnection();
      Statement st = mConnection.createStatement();
      String allMeasurementSitesSql = "SELECT ndwid,name,st_x(location) as lat,st_y(location) as lon from measurementsite;";
      ResultSet rs = st.executeQuery(allMeasurementSitesSql);
      while (rs.next()) {
        String ndwid = rs.getString("ndwid");
        String name = rs.getString("name");
        float lat = rs.getFloat("lat");
        float lon = rs.getFloat("lon");
        MeasurementSite ms = new MeasurementSite();
        ms.setNdwid(ndwid);
        ms.setName(name);
        ms.setLatitude(lat);
        ms.setLongitude(lon);
        msList.add(ms);
      }
      rs.close();
      st.close();
      closeConnection();
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem getting all traffic measurement sites from the database;" + ex.getMessage());
    }
    return msList;
  }

  public List<MeasurementSite> getMeasurementSitesWithinArea(float pBottomLat, float pLeftLon, float pTopLat, float pRightLon) throws RuntimeException {
    List<MeasurementSite> msList = new ArrayList();
    try {
      getConnection();
      Statement st = mConnection.createStatement();
      String allMeasurementSitesSql = "SELECT ndwid,name,st_x(location) as lat,st_y(location) as lon from measurementsite where location && st_makeenvelope(" +
          pBottomLat + ", " + pLeftLon + ", " + pTopLat + ", " + pRightLon + ");";
      LOGGER.finest("Query to get measurement sites within area: " + allMeasurementSitesSql);
      ResultSet rs = st.executeQuery(allMeasurementSitesSql);
      while (rs.next()) {
        String ndwid = rs.getString("ndwid");
        String name = rs.getString("name");
        float lat = rs.getFloat("lat");
        float lon = rs.getFloat("lon");
        MeasurementSite ms = new MeasurementSite();
        ms.setNdwid(ndwid);
        ms.setName(name);
        ms.setLatitude(lat);
        ms.setLongitude(lon);
        msList.add(ms);
      }
      rs.close();
      st.close();
      closeConnection();
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem getting all traffic measurement sites from the database;" + ex.getMessage());
    }
    return msList;
  }

}