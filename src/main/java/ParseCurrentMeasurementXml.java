import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import java.io.*;
import java.util.logging.*;
import java.sql.*;
import java.util.*;


public class ParseCurrentMeasurementXml implements Function<String, List<MeasurementSite>> {
	//private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
  private final static Logger LOGGER = Logger.getLogger(ParseCurrentMeasurementXml.class.getName());

  /**
   * A map containing the different ndw types as they are encoded in the database. To insert measurements 
   * sites into the database the integer code is needed instead of the string. 
   */
  private Map<String, Integer> mNdwTypes = new HashMap<String, Integer>();

  private static Connection conn = null;

	public ParseCurrentMeasurementXml() {
		try {
			CompanionLogger.setup(ParseCurrentMeasurementXml.class.getName());
			LOGGER.setLevel(Level.FINEST);

      getConnection();
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery("SELECT * FROM measurementsitetype");
      while (rs.next())
      {
        int id = rs.getInt("id");
        String ndwType = rs.getString("ndwtype");
        mNdwTypes.put(ndwType, id);
      } 
      rs.close();
      st.close(); 
      closeConnection();
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new RuntimeException("Problem creating log files");
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem connecting to COMPANION database");
    }
	}

  private Connection getConnection() throws RuntimeException {
    if (conn == null) {
      setupDatabaseConnection("snt", "snt");
    }
    return conn;
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
      conn = DriverManager.getConnection(url);
    } catch (ClassNotFoundException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem loading the Postgres JDBC driver");
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem connecting to or manipulating COMPANION database");
    }
  }

  private void closeConnection() {
    try {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new RuntimeException("Problem closing connection to COMPANION database");
    }
  }

  private void addMeasurementSiteToDb(MeasurementSite ms) throws SQLException {
    getConnection();
    String selectSql = "SELECT * FROM measurementsite where ndwid='" + ms.getNdwid() + "'";
    LOGGER.finest("SQL statement to get number of records: " + selectSql);
    Statement st = conn.createStatement();
    // Check first if measurement site already exists in db
    ResultSet rs = st.executeQuery(selectSql);
    LOGGER.finest("Number of records in measurementsite table with id " + ms.getNdwid() +  " = " + rs.getFetchSize());
    if (rs.getFetchSize() == 0) {
      // Does not exist, so add to database
      String selectMaxSql = "SELECT max(id) FROM measurementsite";
      LOGGER.finest("SQL statement to get max id: " + selectMaxSql);
      Statement st2 = conn.createStatement();
      // Check first if measurement site already exists in db
      ResultSet rs2 = st2.executeQuery(selectMaxSql);
      int maxId = 1;
      if (rs2.getFetchSize() > 0) {
        rs2.first();
        maxId = rs.getInt(1) + 1;
      }
      rs2.close();
      st2.close();
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
      String insertSql = "INSERT INTO measurementsite VALUES(" + maxId +  ", '" + ms.getNdwid() + "', '" + ms.getName() + "', " + ms.getNdwtype() + ", '" + ms.getLocation1() + "', " + 
        carriageWay1 + ", " + ms.getLengthaffected1() + ", "  + location2 + ", " + carriageWay2 + ", " + ms.getLengthaffected2() + ")";
      LOGGER.finest(insertSql);
      int rowsAdded = st.executeUpdate(insertSql);
      LOGGER.finest("Rows added: " + rowsAdded);
    } else {
      LOGGER.finest("Row with NDW id " + ms.getNdwid() + " exists already");
    }
    rs.close();
    st.close(); 
  }

  public List<MeasurementSite> call(String pXmlString) {
    // Parse the XML formatted string using the DOM parser which is good to have all elements loaded in memory, but is known not to be the fastest parser
    List<MeasurementSite> measurementSites = new ArrayList<MeasurementSite>();
    try {
      LOGGER.info("Starting to parse current measurements XML file");
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(pXmlString));
      Document doc = db.parse(is);
      doc.getDocumentElement().normalize();

      Node root = doc.getDocumentElement();
      LOGGER.info("Root element :" + root.getNodeName());

      NodeList nodes = doc.getElementsByTagName("payloadPublication");
      // Normally only one element
      if (nodes.getLength() > 1) {
        throw new Exception("More than one payloadPublication element in document");
      } else {
        Element payloadPublicationElement = (Element) nodes.item(0);
        fillPayloadFromDocument(payloadPublicationElement, measurementSites);
      }

      LOGGER.info("Finished parsing current measurements XML file");

      // Fill database with measurement sites
      for (MeasurementSite ms : measurementSites) {
        addMeasurementSiteToDb(ms);
      }
      closeConnection();
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOGGER.severe("Something went wrong trying to add measurements to the database");
    } catch (Exception ex) {
      LOGGER.severe("Something went wrong trying to parse the XML file extracted from the downloaded archive");
      ex.printStackTrace();
    }
    return measurementSites;
  }	

  private void fillPayloadFromDocument(Element payloadPublicationElement, List<MeasurementSite> measurementSites) throws Exception {
    NodeList publicationTimeList = payloadPublicationElement.getElementsByTagName("publicationTime");
    if (publicationTimeList.getLength() > 0) {
      Element publicationTime = (Element) publicationTimeList.item(0);
      LOGGER.info("Publication time: " + XmlUtilities.getCharacterDataFromElement(publicationTime));
    }

    NodeList publicationCreatorList = payloadPublicationElement.getElementsByTagName("publicationCreator");
    if (publicationCreatorList.getLength() > 0) {
      Element publicationCreator = (Element) publicationCreatorList.item(0);
      NodeList countryList = publicationCreator.getElementsByTagName("country");
      if (countryList.getLength() > 0) {
        Element country = (Element) countryList.item(0);
        LOGGER.info("Country : " + XmlUtilities.getCharacterDataFromElement(country));
      }
      NodeList nationalIdentifierList = publicationCreator.getElementsByTagName("nationalIdentifier");
      if (nationalIdentifierList.getLength() > 0) {
        Element nationalIdentifier = (Element) nationalIdentifierList.item(0);
        LOGGER.info("National identifier : " + XmlUtilities.getCharacterDataFromElement(nationalIdentifier));
      }
    }

    NodeList measurementSiteTableList = payloadPublicationElement.getElementsByTagName("measurementSiteTable");
    if (measurementSiteTableList.getLength() > 1) {
      throw new Exception("More than one measurementSiteTable element in document under payloadPublication");
    } else if (measurementSiteTableList.getLength() == 0) {
      throw new Exception("No measurementSiteTable element in document under payloadPublication");
    } else {
      Element measurementSiteTable = (Element) measurementSiteTableList.item(0);
      LOGGER.info("Measurement site table: " + XmlUtilities.getCharacterDataFromElement(measurementSiteTable));
      NodeList measurementSiteRecordList = measurementSiteTable.getElementsByTagName("measurementSiteRecord");
      List<String> knownMsrTags = new ArrayList<String>();
      knownMsrTags.add("measurementSiteRecordVersionTime");
      knownMsrTags.add("computationMethod");
      knownMsrTags.add("measurementEquipmentTypeUsed");
      knownMsrTags.add("measurementSiteName");
      knownMsrTags.add("measurementSiteNumberOfLanes");
      knownMsrTags.add("measurementSpecificCharacteristics");
      knownMsrTags.add("measurementSiteLocation");
      knownMsrTags.add("measurementSide");
      knownMsrTags.add("measurementEquipmentReference");
      // knownMsrTags.add("");
      // knownMsrTags.add("");
      // knownMsrTags.add("");
      if (measurementSiteRecordList.getLength() > 0) {
        LOGGER.info("Measurement site records : " + measurementSiteRecordList.getLength());
        for (int i = 0; i < measurementSiteRecordList.getLength(); i++) {
          Node msrNode = measurementSiteRecordList.item(i);
          createMeasurementSiteRecord(msrNode, knownMsrTags, measurementSites);
        }
      }
    }
  }

  private void createMeasurementSiteRecord(Node msrNode, List<String> knownMsrTags, List<MeasurementSite> measurementSites) {
    Element msrElt = (Element) msrNode;
    for (Element msrEltChild : XmlUtilities.getChildren(msrElt)) {
      String childTagName = msrEltChild.getTagName();
      if (!isContainedInList(knownMsrTags, childTagName)) {
        LOGGER.info("New element under measurementSiteRecord : " + childTagName);
      }
    }
    LOGGER.finest("Measurement site record: " + XmlUtilities.getCharacterDataFromElement(msrElt));
    MeasurementSite ms = new MeasurementSite();
    String ndwId = msrElt.getAttribute("id");
    ms.setNdwid(ndwId);    

    List<Element> msrChildren = XmlUtilities.getChildren(msrElt);
    LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;#Children: " + msrChildren.size());
    NodeList measurementSiteRecordVersionTimeList = msrElt.getElementsByTagName("measurementSiteRecordVersionTime");
    if (measurementSiteRecordVersionTimeList.getLength() > 0) {
      Element measurementSiteRecordVersionTime = (Element) measurementSiteRecordVersionTimeList.item(0);
      LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;Measurement site record version time: " + XmlUtilities.getCharacterDataFromElement(measurementSiteRecordVersionTime));
    }

    NodeList measurementSiteNameList = msrElt.getElementsByTagName("measurementSiteName");
    if (measurementSiteNameList.getLength() > 0) {
      Element measurementSiteName = (Element) measurementSiteNameList.item(0);
      NodeList measurementSiteNameValuesList = measurementSiteName.getElementsByTagName("values");
      if (measurementSiteNameValuesList.getLength() > 0) {
        Element measurementSiteNameValues = (Element) measurementSiteNameValuesList.item(0);
        NodeList measurementSiteNameValuesValueList = measurementSiteNameValues.getElementsByTagName("value");
        if (measurementSiteNameValuesValueList.getLength() > 0) {
          Element measurementSiteNameValuesValue = (Element) measurementSiteNameValuesValueList.item(0);
          String name = XmlUtilities.getCharacterDataFromElement(measurementSiteNameValuesValue);
          LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;Measurement site name: " + name);
          ms.setName(name);
        }
      }
    }

    NodeList measurementSiteLocationList = msrElt.getElementsByTagName("measurementSiteLocation");
    if (measurementSiteLocationList.getLength() > 0) {
      Element measurementSiteLocation = (Element) measurementSiteLocationList.item(0);
      String mslType = measurementSiteLocation.getAttribute("xsi:type");
      if (mNdwTypes.containsKey(mslType)) {
        int mslTypeInt = mNdwTypes.get(mslType);
        ms.setNdwtype(mslTypeInt);
      }
      LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;Measurement site location type: " + mslType);
      List<String> knownMslTags = new ArrayList<String>();
      knownMslTags.add("locationContainedInItinerary");
      knownMslTags.add("locationForDisplay");
      knownMslTags.add("supplementaryPositionalDescription");
      knownMslTags.add("alertCPoint");
      // knownMslTags.add("");
      for (Element mslEltChild : XmlUtilities.getChildren(measurementSiteLocation)) {
        String childTagName = mslEltChild.getTagName();
        if (childTagName.equalsIgnoreCase("locationContainedInItinerary")) {
          // Always only location tag under it
          NodeList locationList = mslEltChild.getElementsByTagName("location");
          if (locationList.getLength() > 0) {
            Element location = (Element) locationList.item(0);
            // Always xsi:type:Linear type
            //LOGGER.finest("Location type : " + XmlUtilities.getCharacterDataFromElement(location));
            List<String> knownLocTags = new ArrayList<String>();
            knownLocTags.add("locationForDisplay");
            knownLocTags.add("supplementaryPositionalDescription");
            knownLocTags.add("alertCLinear");
            knownLocTags.add("linearExtension");
            // knownLocTags.add("");
            for (Element locationChild : XmlUtilities.getChildren(location)) {
              String locationChildTagName = locationChild.getTagName();
              if (locationChildTagName.equalsIgnoreCase("locationForDisplay")) {
                fillLocationForDisplay(locationChild, ms);
              } else if (locationChildTagName.equalsIgnoreCase("supplementaryPositionalDescription")) {
                fillSupplementaryPositionalDescription(locationChild, ms);
              } else if (locationChildTagName.equalsIgnoreCase("linearExtension")) {
                fillLinearExtension(locationChild, ms);
              } else if (!isContainedInList(knownLocTags, locationChildTagName)) {
                LOGGER.info("New element under location : " + locationChildTagName);
              }
            }
          }
        } else if (childTagName.equalsIgnoreCase("locationForDisplay")) {
          fillLocationForDisplay(mslEltChild, ms);
        } else if (childTagName.equalsIgnoreCase("supplementaryPositionalDescription")) {
          fillSupplementaryPositionalDescription(mslEltChild, ms);
        } else if (!isContainedInList(knownMslTags, childTagName)) {
          LOGGER.info("New element under measurementSiteLocation : " + childTagName);
        }
      }
      measurementSites.add(ms);
    }
  }

  private void fillLocationForDisplay(Element locationElement, MeasurementSite ms) {
    NodeList latitudeList = locationElement.getElementsByTagName("latitude");
    NodeList longitudeList = locationElement.getElementsByTagName("longitude");
    if (latitudeList.getLength() > 0 && longitudeList.getLength() > 0) {
      Element latitude = (Element) latitudeList.item(0);
      Element longitude = (Element) longitudeList.item(0);
      String latitudeString = XmlUtilities.getCharacterDataFromElement(latitude); 
      String longitudeString = XmlUtilities.getCharacterDataFromElement(longitude);
      try {
        double lat = Double.valueOf(latitudeString);
        double lon = Double.valueOf(longitudeString);
        ms.addCoordinateToLocation1(lat, lon);
      } catch (Exception ex) {
        ex.printStackTrace();
        LOGGER.severe("Latitude and longitude not properly formed as numeric values; " + ex.getMessage());
      }
      LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;Location for display (lat, lon) = (" + latitudeString + ", " + longitudeString + ")");
    }
  }

  private void fillSupplementaryPositionalDescription(Element supplementaryPositionalDescriptionElement, MeasurementSite ms) {
    List<String> knownSpdTags = new ArrayList<String>();
    knownSpdTags.add("affectedCarriagewayAndLanes");
    for (Element supplementaryPositionalDescriptionChild : XmlUtilities.getChildren(supplementaryPositionalDescriptionElement)) {
      String supplementaryPositionalDescriptionChildTagName = supplementaryPositionalDescriptionChild.getTagName();
      if (supplementaryPositionalDescriptionChildTagName.equalsIgnoreCase("affectedCarriagewayAndLanes")) {
        NodeList carriageWayAndLanesList = supplementaryPositionalDescriptionElement.getElementsByTagName("affectedCarriagewayAndLanes");
        for (int cwlIndex = 0; cwlIndex < carriageWayAndLanesList.getLength(); cwlIndex++ ) {
          Element carriageWayAndLanes = (Element) carriageWayAndLanesList.item(cwlIndex);
          NodeList lengthAffectedList = carriageWayAndLanes.getElementsByTagName("lengthAffected");
          NodeList carriageWayList = carriageWayAndLanes.getElementsByTagName("carriageway");
          String lengthAffectedString = "length affected = N/A";
          String carriageWayString = "carriage way : N/A";
          if (lengthAffectedList.getLength() > 0) {
            Element lengthAffected = (Element) lengthAffectedList.item(0);
            lengthAffectedString = XmlUtilities.getCharacterDataFromElement(lengthAffected);
            try {
              Integer lengthAffectedValue = Integer.valueOf(lengthAffectedString);
              ms.setLengthaffected1(lengthAffectedValue);
            } catch (Exception ex) {
              // Ignore
            }
            lengthAffectedString = "length affected = " + XmlUtilities.getCharacterDataFromElement(lengthAffected);
          }
          if (carriageWayList.getLength() > 0) {
            Element carriageWay = (Element) carriageWayList.item(0);
            carriageWayString = XmlUtilities.getCharacterDataFromElement(carriageWay);
            ms.setCarriageway1(carriageWayString);
            carriageWayString = "carriage way : " + XmlUtilities.getCharacterDataFromElement(carriageWay);
          }
          LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;" + carriageWayString + ", " + lengthAffectedString);
        }
      } else if (!isContainedInList(knownSpdTags, supplementaryPositionalDescriptionChildTagName)) {
        LOGGER.info("New element under supplementary positional description in location: " + supplementaryPositionalDescriptionChildTagName);
      }
    }
  }

  private void fillLinearExtension(Element linearExtensionElement, MeasurementSite ms) {
    List<String> knownLeTags = new ArrayList<String>();
    knownLeTags.add("linearByCoordinatesExtension");
    for (Element linearExtensionChild : XmlUtilities.getChildren(linearExtensionElement)) {
      String linearExtensionChildTagName = linearExtensionChild.getTagName();
      if (linearExtensionChildTagName.equalsIgnoreCase("linearByCoordinatesExtension")) {
        NodeList linearByCoordinatesExtensionList = linearExtensionElement.getElementsByTagName("linearByCoordinatesExtension");
        if (linearByCoordinatesExtensionList.getLength() > 0) {
          Element linearByCoordinatesExtension = (Element) linearByCoordinatesExtensionList.item(0);
          fillCoordinatesFromElement(linearByCoordinatesExtension, ms);
        }
      } else if (!isContainedInList(knownLeTags, linearExtensionChildTagName)) {
        LOGGER.info("New element under linear extension in location: " + linearExtensionChildTagName);
      }
    }
  }

  private void fillCoordinatesFromElement(Element linearByCoordinatesExtension, MeasurementSite ms) {
    NodeList startPointList = linearByCoordinatesExtension.getElementsByTagName("linearCoordinatesStartPoint");
    NodeList endPointList = linearByCoordinatesExtension.getElementsByTagName("linearCoordinatesEndPoint");
    String startCoordinateString = "start coordinate (lat,lon) = (";
    if (startPointList.getLength() > 0) {
      Element startPoint = (Element) startPointList.item(0);
      NodeList startPointCoordinateList = startPoint.getElementsByTagName("pointCoordinates");
      if (startPointCoordinateList.getLength() > 0) {
        Element startPointCoordinate = (Element) startPointCoordinateList.item(0);
        NodeList latitudeList = startPointCoordinate.getElementsByTagName("latitude");
        NodeList longitudeList = startPointCoordinate.getElementsByTagName("longitude");
        if (latitudeList.getLength() > 0 && longitudeList.getLength() > 0) {
          Element latitude = (Element) latitudeList.item(0);
          Element longitude = (Element) longitudeList.item(0);
          startCoordinateString += XmlUtilities.getCharacterDataFromElement(latitude) + ", " + 
              XmlUtilities.getCharacterDataFromElement(longitude);
        }
      }
    }
    startCoordinateString += ")";
    String endCoordinateString = "end coordinate (lat,lon) = (";
    if (endPointList.getLength() > 0) {
      Element endPoint = (Element) endPointList.item(0);
      NodeList endPointCoordinateList = endPoint.getElementsByTagName("pointCoordinates");
      if (endPointCoordinateList.getLength() > 0) {
        Element endPointCoordinate = (Element) endPointCoordinateList.item(0);
        NodeList latitudeList = endPointCoordinate.getElementsByTagName("latitude");
        NodeList longitudeList = endPointCoordinate.getElementsByTagName("longitude");
        if (latitudeList.getLength() > 0 && longitudeList.getLength() > 0) {
          Element latitude = (Element) latitudeList.item(0);
          Element longitude = (Element) longitudeList.item(0);
          endCoordinateString += XmlUtilities.getCharacterDataFromElement(latitude) + ", " + 
              XmlUtilities.getCharacterDataFromElement(longitude);
        }
      }
    }
    endCoordinateString += ")";
    LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;Coordinates : " + startCoordinateString + ", " + endCoordinateString);
  }

  private boolean isContainedInList(List<String> valueList, String searchString) {
    for (String curVal : valueList) {
      if (curVal.equalsIgnoreCase(searchString)) {
        return true;
      }
    }
    return false;
  }

}