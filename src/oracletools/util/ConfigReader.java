package oracletools.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ConfigReader {
	public Map<String, String> read(String fileName) throws ParserConfigurationException, SAXException, IOException {
		Map<String, String> properties = new HashMap<String, String>();
		
		File file = new File(fileName);  
		DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();  
		Document document = documentBuilder.parse(file);  
		NodeList nodeList = document.getChildNodes().item(0).getChildNodes();
		for(int i=0;i<nodeList.getLength();i++) {
			Node node = nodeList.item(i);
			properties.put(node.getNodeName(),node.getTextContent());
		}
				
		return properties;

	}
}
