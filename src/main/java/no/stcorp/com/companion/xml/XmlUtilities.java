package no.stcorp.com.companion.xml;

import org.w3c.dom.*;
import java.util.*;

public class XmlUtilities {

  /**
   * A method to get the contents of an element
   */
  public static String getCharacterDataFromElement(Element e) {
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

  public static List<Element> getChildren(Element element) {
    List<Element> elements = new ArrayList<>();
    NodeList nodeList = element.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
        Node node = nodeList.item(i);
        if (node.getNodeType() == Node.ELEMENT_NODE) {
            elements.add((Element) node);
        }
    }
    return elements;
  }

}