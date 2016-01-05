import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.time.*;
import java.time.format.*;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;import javax.xml.transform.stream.StreamResult;


/**
 * A class for generating a KML file for different sets of data (weather stations, traffic measurement sites)
 */
public class KmlGenerator {
	
	private final static Logger LOGGER = Logger.getLogger(KmlGenerator.class.getName());

	public KmlGenerator() {
 		try {
			CompanionLogger.setup(KmlGenerator.class.getName());
			LOGGER.setLevel(Level.INFO);
	    LOGGER.info("Creating KMl generator");
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new RuntimeException("Problem creating log files;" + ex.getMessage());
    }
	}

	public void generateKmlForWeatherStations(List<WeatherStation> weatherStations) {
		LOGGER.info("Generating KML file for weather stations");
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      TransformerFactory tranFactory = TransformerFactory.newInstance();
      Transformer aTransformer = tranFactory.newTransformer();

      Document doc = builder.newDocument();
      Element root = doc.createElement("kml");
      root.setAttribute("xmlns", "http://www.opengis.net/kml/2.2");
      root.setAttribute("xmlns:gx", "http://www.google.com/kml/ext/2.2");
      root.setAttribute("xmlns:kml", "http://www.opengis.net/kml/2.2");
      root.setAttribute("xmlns:atom", "http://www.w3.org/2005/Atom");
      doc.appendChild(root);
      Element dnode = doc.createElement("Document");
      root.appendChild(dnode);
      Element wstyle = doc.createElement("Style");
      wstyle.setAttribute("id", "weatherStation");
      Element wistyle = doc.createElement("IconStyle");
      wistyle.setAttribute("id", "weatherIcon");
      Element wscale = doc.createElement("scale");
      wscale.appendChild(doc.createTextNode("1"));

      Element wicon = doc.createElement("Icon");
      Element wiconhref = doc.createElement("href");
      wiconhref.appendChild(doc.createTextNode("http://maps.google.com/mapfiles/kml/pal4/icon22.png"));

      wstyle.appendChild(wistyle);
      wistyle.appendChild(wscale);
      wicon.appendChild(wiconhref);
      wistyle.appendChild(wicon);
      dnode.appendChild(wstyle);

      System.out.println("Number of weather stations: " + weatherStations.size());
      for (WeatherStation weatherStation : weatherStations) {
        Element placemark = doc.createElement("Placemark");
        dnode.appendChild(placemark);
        Element name = doc.createElement("name");
        name.appendChild(doc.createTextNode(weatherStation.getName()));
        placemark.appendChild(name);
        Element styleUrl = doc.createElement("styleUrl");
        styleUrl.appendChild(doc.createTextNode("#weatherStation"));
        placemark.appendChild(styleUrl);
        Element point = doc.createElement("Point");
				Element coordinates = doc.createElement("coordinates");
        coordinates.appendChild(doc.createTextNode(weatherStation.getLongitude() + "," + weatherStation.getLatitude() + "," + weatherStation.getAltitude()));
        point.appendChild(coordinates);
				placemark.appendChild(point);
				Element description = doc.createElement("description");
        description.appendChild(doc.createTextNode("KNMI id: " + weatherStation.getKnmiId()));
        placemark.appendChild(description);        
      }
      Source src = new DOMSource(doc);
      Instant now = Instant.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault());
      Result dest = new StreamResult(File.createTempFile("WeatherStations_" + formatter.format(now), ".kml"));
      aTransformer.transform(src, dest);
      LOGGER.info("Completed.....");
    } catch (Exception ex) {
    	ex.printStackTrace();
      LOGGER.severe(ex.getMessage());
    }
	}

	public void generateKmlForMeasurementSites(List<MeasurementSite> measurementSites) {
		LOGGER.info("Generating KML file for traffic measurement sites");
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      TransformerFactory tranFactory = TransformerFactory.newInstance();
      Transformer aTransformer = tranFactory.newTransformer();

      Document doc = builder.newDocument();
      Element root = doc.createElement("kml");
      root.setAttribute("xmlns", "http://www.opengis.net/kml/2.2");
      root.setAttribute("xmlns:gx", "http://www.google.com/kml/ext/2.2");
      root.setAttribute("xmlns:kml", "http://www.opengis.net/kml/2.2");
      root.setAttribute("xmlns:atom", "http://www.w3.org/2005/Atom");
      doc.appendChild(root);
      Element dnode = doc.createElement("Document");
      root.appendChild(dnode);
      Element wstyle = doc.createElement("Style");
      wstyle.setAttribute("id", "measurementSite");
      Element wistyle = doc.createElement("IconStyle");
      wistyle.setAttribute("id", "weatherIcon");
      Element wscale = doc.createElement("scale");
      wscale.appendChild(doc.createTextNode("0.2"));

      Element wicon = doc.createElement("Icon");
      Element wiconhref = doc.createElement("href");
      wiconhref.appendChild(doc.createTextNode("http://maps.google.com/mapfiles/kml/pal4/icon15.png"));

      wstyle.appendChild(wistyle);
      wistyle.appendChild(wscale);
      wicon.appendChild(wiconhref);
      wistyle.appendChild(wicon);
      dnode.appendChild(wstyle);

      System.out.println("Number of measurement sites: " + measurementSites.size());
      for (MeasurementSite measurementSite : measurementSites) {
        Element placemark = doc.createElement("Placemark");
        dnode.appendChild(placemark);
        Element name = doc.createElement("name");
        name.appendChild(doc.createTextNode(measurementSite.getName()));
        placemark.appendChild(name);
        Element styleUrl = doc.createElement("styleUrl");
        styleUrl.appendChild(doc.createTextNode("#measurementSite"));
        placemark.appendChild(styleUrl);
        Element point = doc.createElement("Point");
				Element coordinates = doc.createElement("coordinates");
        coordinates.appendChild(doc.createTextNode(measurementSite.getLongitude() + "," + measurementSite.getLatitude()));
        point.appendChild(coordinates);
				placemark.appendChild(point);
				Element description = doc.createElement("description");
        description.appendChild(doc.createTextNode("NDW id: " + measurementSite.getNdwid()));
        placemark.appendChild(description);        
      }
      Source src = new DOMSource(doc);
      Instant now = Instant.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault());
      Result dest = new StreamResult(File.createTempFile("MeasurementSites_" + formatter.format(now), ".kml"));
      aTransformer.transform(src, dest);
      LOGGER.info("Completed.....");
    } catch (Exception ex) {
    	ex.printStackTrace();
      LOGGER.severe(ex.getMessage());
    }
	}
}