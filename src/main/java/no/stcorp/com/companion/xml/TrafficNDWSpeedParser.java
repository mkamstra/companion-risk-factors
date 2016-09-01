package no.stcorp.com.companion.xml;

import no.stcorp.com.companion.database.DatabaseManager;
import no.stcorp.com.companion.logging.*;
import no.stcorp.com.companion.traffic.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.*;

import java.util.*;
import javax.annotation.Nullable;
import javax.lang.model.type.NullType;
import javax.xml.parsers.*;

import org.w3c.dom.*;
import org.xml.sax.InputSource;

import java.io.*;
import java.util.logging.*;


public class TrafficNDWSpeedParser implements Function<String, List<SiteMeasurement>> {
    //private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private final static Logger LOGGER = Logger.getLogger(TrafficNDWSpeedParser.class.getName());
    private static final long serialVersionUID = 3L;
    private DatabaseManager mDbMgr = null;
    private Set<String> mMeasurementSiteIds;

    public TrafficNDWSpeedParser() {
        LOGGER.setLevel(Level.FINE);
        mDbMgr = DatabaseManager.getInstance();
        mMeasurementSiteIds = mDbMgr.getAllMeasurementSiteIdsFromDb();
    }

    public TrafficNDWSpeedParser(float p1_lat, float p1_lon, float p2_lat, float p2_lon) {
        LOGGER.setLevel(Level.FINE);
        mDbMgr = DatabaseManager.getInstance();
        mMeasurementSiteIds = mDbMgr.getLocalMeasurementSiteIdsFromDb(p1_lat, p1_lon, p2_lat, p2_lon);
    }

    public List<SiteMeasurement> call(String pXmlString) {
        // Parse the XML formatted string using the DOM parser which is good to have all elements loaded in memory, but is known not to be the fastest parser
        List<SiteMeasurement> siteMeasurements = new ArrayList<SiteMeasurement>();
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(pXmlString));
            Document doc = db.parse(is);
            doc.getDocumentElement().normalize();

            LOGGER.info("Root element :" + doc.getDocumentElement().getNodeName());

            NodeList nodes = doc.getElementsByTagName("payloadPublication");
            // Normally only one element
            if (nodes.getLength() > 1) {
                throw new Exception("More than one payloadPublication element in document");
            } else {
                Element payloadPublicationElement = (Element) nodes.item(0);

                NodeList publicationTime = payloadPublicationElement.getElementsByTagName("publicationTime");
                Element line = (Element) publicationTime.item(0);
                LOGGER.info("Publication time: " + XmlUtilities.getCharacterDataFromElement(line));

                NodeList siteMeasurementNodes = payloadPublicationElement.getElementsByTagName("siteMeasurements");
                LOGGER.info("Number of traffic speed measurements: " + siteMeasurementNodes.getLength());
                for (int i = 0; i < siteMeasurementNodes.getLength(); i++) {
                    Element siteMeasurementElement = (Element) siteMeasurementNodes.item(i);
                    // LOGGER.info("  Measurement "  + i);
                    NodeList siteReferenceNodes = siteMeasurementElement.getElementsByTagName("measurementSiteReference");
                    if (siteReferenceNodes.getLength() > 1) {
                        LOGGER.severe("&nbsp;&nbsp;siteMeasurements " + i + " has more than one node measurementSiteReference");
                    } else if (siteReferenceNodes.getLength() < 1) {
                        LOGGER.severe("&nbsp;siteMeasurements " + i + " has no node measurementSiteReference");
                    } else {
                        Element siteReferenceElement = (Element) siteReferenceNodes.item(0);
                        String siteReferenceIdString = siteReferenceElement.getAttribute("id");

                        LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;site reference id: " + siteReferenceIdString);

                        if (!mMeasurementSiteIds.contains(siteReferenceIdString)) {  // Skip not selected sites
                            continue;
                        }

                        NodeList timeDefaultNodes = siteMeasurementElement.getElementsByTagName("measurementTimeDefault");
                        if (timeDefaultNodes.getLength() > 1) {
                            LOGGER.severe("&nbsp;&nbsp;siteMeasurements " + i + " has more than one node measurementTimeDefault");
                            continue;
                        } else if (timeDefaultNodes.getLength() < 1) {
                            LOGGER.severe("&nbsp;&nbsp;siteMeasurements " + i + " has no node measurementTimeDefault");
                            continue;
                        } else {
                            String timeDefault = XmlUtilities.getCharacterDataFromElement((Element) timeDefaultNodes.item(0));
                            LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;Measurement time default: " + timeDefault);

                            SiteMeasurement sm = new SiteMeasurement(siteReferenceIdString, timeDefault);

                            NodeList measuredValuesNodes = siteMeasurementElement.getElementsByTagName("measuredValue");
                            LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;Number of measured values: " + measuredValuesNodes.getLength());

                            for (int j = 0; j < measuredValuesNodes.getLength(); j++) {
                                Element measuredValue = (Element) measuredValuesNodes.item(j);
                                if (measuredValue.getParentNode() != siteMeasurementElement)
                                    continue;

                                LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Measured value: " + XmlUtilities.getCharacterDataFromElement(measuredValue));

                                String mvIndexString = measuredValue.getAttribute("index");
                                int mvIndex = -1;
                                try {
                                    mvIndex = Integer.parseInt(mvIndexString);
                                } catch (Exception ex) {
                                    LOGGER.severe("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Index not formatted as integer: " + mvIndexString);
                                    ex.printStackTrace();
                                }

                                NodeList measuredValuesNestedNodes = measuredValue.getElementsByTagName("measuredValue");
                                if (measuredValuesNestedNodes.getLength() >= 1) {
                                    Element nestedMeasuredValue = (Element) measuredValuesNestedNodes.item(0);
                                    NodeList basicDataNodes = nestedMeasuredValue.getElementsByTagName("basicData");
                                    if (basicDataNodes.getLength() >= 1) {
                                        Element basicData = (Element) basicDataNodes.item(0);
                                        LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Measured value basic data: " + XmlUtilities.getCharacterDataFromElement(basicData));

                                        String type = basicData.getAttribute("xsi:type");

                                        NodeList vehicleFlowNodes = basicData.getElementsByTagName("vehicleFlow");
                                        if (vehicleFlowNodes.getLength() >= 1) {
                                            Element vehicleFlow = (Element) vehicleFlowNodes.item(0);
                                            NodeList vehicleFlowRateNodes = vehicleFlow.getElementsByTagName("vehicleFlowRate");
                                            if (vehicleFlowRateNodes.getLength() >= 1) {
                                                Element vehicleFlowRate = (Element) vehicleFlowRateNodes.item(0);
                                                String flowRateString = XmlUtilities.getCharacterDataFromElement(vehicleFlowRate);
                                                LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Flow rate: " + flowRateString);
                                                double flowRate = -1.0;
                                                try {
                                                    flowRate = Double.parseDouble(flowRateString);
                                                } catch (Exception ex) {
                                                    LOGGER.severe("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Flow rate not formatted as a double: " + flowRate);
                                                    ex.printStackTrace();
                                                }
                                                sm.addMeasuredValue(mvIndex, type, flowRate);
                                            }
                                        } else {
                                            NodeList trafficSpeedNodes = basicData.getElementsByTagName("averageVehicleSpeed");
                                            if (trafficSpeedNodes.getLength() >= 1) {
                                                Element averageVehicleSpeed = (Element) trafficSpeedNodes.item(0);
                                                NodeList averageVehicleSpeedNodes = averageVehicleSpeed.getElementsByTagName("speed");
                                                if (averageVehicleSpeedNodes.getLength() >= 1) {
                                                    Element speedElement = (Element) averageVehicleSpeedNodes.item(0);
                                                    String speedString = XmlUtilities.getCharacterDataFromElement(speedElement);
                                                    LOGGER.finest("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Speed: " + speedString);
                                                    double speed = -1.0;
                                                    try {
                                                        speed = Double.parseDouble(speedString);
                                                    } catch (Exception ex) {
                                                        LOGGER.severe("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Speed not formatted as a double: " + speedString);
                                                        ex.printStackTrace();
                                                    }
                                                    sm.addMeasuredValue(mvIndex, type, speed);
                                                }
                                            } else {
                                                LOGGER.severe("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;No vehicle flow data or average vehicle speed");
                                            }
                                        }
                                    }
                                }
                            }
                            siteMeasurements.add(sm);
                        }
                    }
                    // try {
                    //   Thread.sleep(10000);
                    // } catch (InterruptedException ex) {
                    //   LOGGER.severe("Something went wrong putting the app to sleep for 10 seconds");
                    //   ex.printStackTrace();
                    //   Thread.currentThread().interrupt();
                    // }
                }
            }
        } catch (Exception ex) {
            LOGGER.severe("Something went wrong trying to parse the XML file extracted from the downloaded archive");
            ex.printStackTrace();
        }
        return siteMeasurements;
    }

}