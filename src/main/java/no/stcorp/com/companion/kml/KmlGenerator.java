package no.stcorp.com.companion.kml;

import no.stcorp.com.companion.logging.*;
import no.stcorp.com.companion.traffic.*;
import no.stcorp.com.companion.weather.*;

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
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;


/**
 * A class for generating a KML file for different sets of data (weather stations, traffic measurement sites)
 */
public class KmlGenerator {
	
	private final static Logger LOGGER = Logger.getLogger(KmlGenerator.class.getName());

	public KmlGenerator() {
		LOGGER.setLevel(Level.INFO);
    LOGGER.info("Creating KMl generator");
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
      wscale.appendChild(doc.createTextNode("0.4"));

      Element wicon = doc.createElement("Icon");
      Element wiconhref = doc.createElement("href");
      wiconhref.appendChild(doc.createTextNode("http://maps.google.com/mapfiles/kml/pal4/icon15.png"));

      wstyle.appendChild(wistyle);
      wistyle.appendChild(wscale);
      wicon.appendChild(wiconhref);
      wistyle.appendChild(wicon);
      dnode.appendChild(wstyle);

      LOGGER.info("Number of measurement sites: " + measurementSites.size());
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
        coordinates.appendChild(doc.createTextNode(measurementSite.getLocation().getLongitude() + "," + measurementSite.getLocation().getLatitude()));
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

  public void generateKmlForMeasurementSitesWithSegments(List<MeasurementSiteSegment> measurementSites) {
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
      wscale.appendChild(doc.createTextNode("0.4"));

      Element wicon = doc.createElement("Icon");
      Element wiconhref = doc.createElement("href");
      wiconhref.appendChild(doc.createTextNode("http://maps.google.com/mapfiles/kml/pal4/icon15.png"));

      wstyle.appendChild(wistyle);
      wistyle.appendChild(wscale);
      wicon.appendChild(wiconhref);
      wistyle.appendChild(wicon);

      Element lineStyle = doc.createElement("LineStyle");
      Element physicalWidth = doc.createElement("gx:physicalWidth");
      physicalWidth.appendChild(doc.createTextNode("12"));
      lineStyle.appendChild(physicalWidth);
      Element labelVisibility = doc.createElement("gx:labelVisibility");
      labelVisibility.appendChild(doc.createTextNode("1"));
      lineStyle.appendChild(labelVisibility);
      wstyle.appendChild(lineStyle);
      dnode.appendChild(wstyle);

      LOGGER.info("Number of measurement sites: " + measurementSites.size());
      for (MeasurementSiteSegment measurementSite : measurementSites) {
        Element placemark = doc.createElement("Placemark");
        dnode.appendChild(placemark);
        Element name = doc.createElement("name");
        name.appendChild(doc.createTextNode(measurementSite.getName()));
        placemark.appendChild(name);
        Element styleUrl = doc.createElement("styleUrl");
        styleUrl.appendChild(doc.createTextNode("#measurementSite"));
        placemark.appendChild(styleUrl);
        Element lineString = doc.createElement("LineString");
        Element extrude = doc.createElement("extrude");
        extrude.appendChild(doc.createTextNode("1"));
        lineString.appendChild(extrude);
        Element tessellate = doc.createElement("tessellate");
        tessellate.appendChild(doc.createTextNode("1"));
        lineString.appendChild(tessellate);
        Element altitudeMode = doc.createElement("altitudeMode");
        altitudeMode.appendChild(doc.createTextNode("relativeToGround"));
        lineString.appendChild(altitudeMode);
        Element coordinates = doc.createElement("coordinates");
        String coordinatesString = "";
        for (Location loc : measurementSite.getCoordinates()) {
          coordinatesString += loc.getLongitude() + "," + loc.getLatitude() + "," + loc.getAltitude() + " ";
        }
        coordinatesString = coordinatesString.trim();
        //System.out.println("Coordinates KML: " + coordinatesString);
        coordinates.appendChild(doc.createTextNode(coordinatesString));
        lineString.appendChild(coordinates);
        placemark.appendChild(lineString);
        Element description = doc.createElement("description");
        description.appendChild(doc.createTextNode("NDW id: " + measurementSite.getNdwid()));
        placemark.appendChild(description);        
      }
      Source src = new DOMSource(doc);
      Instant now = Instant.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault());
      Result dest = new StreamResult(File.createTempFile("MeasurementSitesWithSegments_" + formatter.format(now), ".kml"));
      aTransformer.transform(src, dest);
      LOGGER.info("Completed.....");
    } catch (Exception ex) {
      ex.printStackTrace();
      LOGGER.severe(ex.getMessage());
    }
  }
}