import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.*;
import java.util.List;
import java.util.ArrayList;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import java.io.*;
import java.util.logging.*;


public class ParseCurrentMeasurementXml implements Function<String, List<SiteMeasurement>> {
	private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

	public ParseCurrentMeasurementXml() {
		try {
			CompanionLogger.setup();
			LOGGER.setLevel(Level.FINEST);
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new RuntimeException("Problem creating log files");
		}
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

      System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

      // NodeList nodes = doc.getElementsByTagName("payloadPublication");
      // // Normally only one element
      // if (nodes.getLength() > 1) {
      //   throw new Exception("More than one payloadPublication element in document");
      // } else {
      //   Element payloadPublicationElement = (Element) nodes.item(0);

      //   NodeList publicationTime = payloadPublicationElement.getElementsByTagName("publicationTime");
      //   Element line = (Element) publicationTime.item(0);
      //   LOGGER.info("Publication time: " + getCharacterDataFromElement(line));

      //   NodeList siteMeasurementNodes = payloadPublicationElement.getElementsByTagName("siteMeasurements");
      //   LOGGER.info("Number of site measurements: " + siteMeasurementNodes.getLength());
      //   int maxPrintElements = 20;
      //   for (int i = 0; i < siteMeasurementNodes.getLength(); i++) {
      //     Element siteMeasurementElement = (Element) siteMeasurementNodes.item(i);
      //     // LOGGER.info("  Measurement "  + i);
      //     NodeList siteReferenceNodes = siteMeasurementElement.getElementsByTagName("measurementSiteReference");
      //     if (siteReferenceNodes.getLength() > 1) {
      //       LOGGER.severe("  siteMeasurements " + i + " has more than one node measurementSiteReference");
      //     } else if (siteReferenceNodes.getLength() < 1) {
      //       LOGGER.severe("  siteMeasurements " + i + " has no node measurementSiteReference");
      //     } else {
      //       String siteReferenceString = getCharacterDataFromElement((Element) siteReferenceNodes.item(0));
      //       if (i < maxPrintElements)
      //       	LOGGER.finest("    Measurement site reference: " + siteReferenceString);

      //       NodeList timeDefaultNodes = siteMeasurementElement.getElementsByTagName("measurementTimeDefault");
      //       if (timeDefaultNodes.getLength() > 1) {
      //         LOGGER.severe("  siteMeasurements " + i + " has more than one node measurementTimeDefault");
      //       } else if (timeDefaultNodes.getLength() < 1) {
      //         LOGGER.severe("  siteMeasurements " + i + " has no node measurementTimeDefault");
      //       } else {
      //         String timeDefault = getCharacterDataFromElement((Element) timeDefaultNodes.item(0));
      //       	if (i < maxPrintElements)
      //         	LOGGER.finest("    Measurement time default: " + timeDefault);

      //         SiteMeasurement sm = new SiteMeasurement(siteReferenceString, timeDefault);

      //         NodeList measuredValuesNodes = siteMeasurementElement.getElementsByTagName("measuredValue");
	     //        if (i < maxPrintElements)
  	   //          LOGGER.finest("    Number of measured values: " + measuredValuesNodes.getLength());

      //         for (int j= 0 ; j < measuredValuesNodes.getLength(); j++) {
      //           Element measuredValue = (Element) measuredValuesNodes.item(j);
      //           if (measuredValue.getParentNode() != siteMeasurementElement) 
      //           	continue;

		    //         if (i < maxPrintElements)
    	 //            LOGGER.finest("      Measured value: " + getCharacterDataFromElement(measuredValue));

      //           String mvIndexString = measuredValue.getAttribute("index");
      //           int mvIndex = -1;
      //           try {
      //             mvIndex = Integer.parseInt(mvIndexString);
      //           } catch (Exception ex) {
      //             LOGGER.severe("Index not formatted as integer: " + mvIndexString);
      //             ex.printStackTrace();
      //           }

      //           NodeList measuredValuesNestedNodes = measuredValue.getElementsByTagName("measuredValue");
      //           if (measuredValuesNestedNodes.getLength() >= 1) {
      //             Element nestedMeasuredValue = (Element) measuredValuesNestedNodes.item(0);
      //             NodeList basicDataNodes = nestedMeasuredValue.getElementsByTagName("basicData");
      //             if (basicDataNodes.getLength() >= 1) {
      //               Element basicData = (Element) basicDataNodes.item(0);
				  //           if (i < maxPrintElements)
      //   	            LOGGER.finest("        Measured value basic data: " + getCharacterDataFromElement(basicData));

      //               String type = basicData.getAttribute("xsi:type");

      //               NodeList vehicleFlowNodes = basicData.getElementsByTagName("vehicleFlow");
      //               if (vehicleFlowNodes.getLength() >= 1) {
      //                 Element vehicleFlow = (Element) vehicleFlowNodes.item(0);
      //                 NodeList vehicleFlowRateNodes = vehicleFlow.getElementsByTagName("vehicleFlowRate");
      //                 if (vehicleFlowRateNodes.getLength() >= 1) {
      //                   Element vehicleFlowRate = (Element) vehicleFlowRateNodes.item(0);
      //                   String flowRateString = getCharacterDataFromElement(vehicleFlowRate);
						//             if (i < maxPrintElements)
      //       	            LOGGER.finest("          Flow rate: " + flowRateString);

      //                   double flowRate = -1.0;
      //                   try {
      //                   	flowRate = Double.parseDouble(flowRateString);
      //                   } catch (Exception ex) {
      //                   	LOGGER.severe("Flow rate not formatted as a double: " + flowRate);
      //                   	ex.printStackTrace();
      //                   }
      //                   sm.addMeasuredValue(mvIndex, type, flowRate);
      //                 }
      //               } else {
      //                 NodeList trafficSpeedNodes = basicData.getElementsByTagName("averageVehicleSpeed");
      //                 if (trafficSpeedNodes.getLength() >= 1) {
      //                   Element averageVehicleSpeed = (Element) trafficSpeedNodes.item(0);
      //                   NodeList averageVehicleSpeedNodes = averageVehicleSpeed.getElementsByTagName("speed");
      //                   if (averageVehicleSpeedNodes.getLength() >= 1) {
      //                     Element speedElement = (Element) averageVehicleSpeedNodes.item(0);
      //                     String speedString = getCharacterDataFromElement(speedElement);
						// 	            if (i < maxPrintElements)
      //         	         		LOGGER.finest("          Speed: " + speedString);

	     //                   	double speed = -1.0;
	     //                  	try {
	     //                  		speed = Double.parseDouble(speedString);
	     //                  	} catch (Exception ex) {
	     //                  		LOGGER.severe("Speed not formatted as a double: " + speedString);
	     //                  		ex.printStackTrace();
	     //                  	}
      //                     sm.addMeasuredValue(mvIndex, type, speed);
      //                   }
      //                 } else {
      //                   LOGGER.severe("        No vehicle flow data or average vehicle speed");
      //                 }
      //               }
      //             }
      //           }
      //         }
      //       }
      //     }
      //     // try {
      //     //   Thread.sleep(10000);
      //     // } catch (InterruptedException ex) {
      //     //   LOGGER.severe("Something went wrong putting the app to sleep for 10 seconds");
      //     //   ex.printStackTrace();
      //     //   Thread.currentThread().interrupt();
      //     // }
      //   }
      // }
    } catch (Exception ex) {
      LOGGER.severe("Something went wrong trying to parse the XML file extracted from the downloaded archive");
      ex.printStackTrace();
    }
    return siteMeasurements;
  }	


  /**
   * A method to get the contents of an element
   */
  private String getCharacterDataFromElement(Element e) {
    Node child = e.getFirstChild();
    if (child instanceof CharacterData) {
      CharacterData cd = (CharacterData) child;
      return cd.getData();
    }
    // If this part of the code is reached, then check for attributes instead
    String attributeListString = "";
    NamedNodeMap attributes = e.getAttributes();
    // System.out.println("Number of attributes: " + attributes.getLength());
    // System.out.println(e.toString());
    for (int i = 0; i < attributes.getLength(); i++) {
      Attr attr = (Attr) attributes.item(i);
      String attributeString = attr.getNodeName() + ":" + attr.getNodeValue();
      attributeListString += attributeString + ", ";
    }
    if (attributeListString.length() > 0) {
      return attributeListString.substring(0, attributeListString.length() - 2); // Remove last comma
    }
    return "?";
  }


}