//package com.akshay.xml.main;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.Iterator;
//
//import javax.xml.stream.XMLEventReader;
//import javax.xml.stream.XMLInputFactory;
//import javax.xml.stream.XMLStreamException;
//import javax.xml.stream.events.Attribute;
//import javax.xml.stream.events.EndElement;
//import javax.xml.stream.events.StartElement;
//import javax.xml.stream.events.XMLEvent;
//
//import org.apache.commons.collections.IteratorUtils;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructType;
//
//import com.akshay.xml.streaming.StreamingXMLReader;
//import com.akshay.xml.utilities.Utils;
//
//import scala.collection.JavaConverters;
//
//public class Test {
//
//	@SuppressWarnings("unchecked")
//	public static StructType inferSchema() {
//		InputStream inputStream;
//		try {
//			inputStream = new FileInputStream(
//					"/Users/akshay.sharma/Downloads/noteBookRepo/AzureDevOps-DBRepo/test.xml");
//			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
//			XMLEventReader reader = inputFactory.createXMLEventReader(inputStream);
//			StructType schema = new StructType();
//			boolean schemaFlag = false;
//			while (reader.hasNext()) {
//				XMLEvent event = (XMLEvent) reader.next();
//
//				if (event.isStartElement()) {
//					StartElement element = event.asStartElement();
//					event = (XMLEvent) reader.next();
//					if (element.getName().getLocalPart().equals("person")) {
//						schemaFlag = true;
//						continue;
//					}
//					if (schemaFlag) {
//						int count = 0;
//						if (event.isCharacters()) {
//							Iterator<Attribute> i = element.getAttributes();
//							StructType subSchema = new StructType();
//							while (i.hasNext()) {
//
//								count += 1;
//								Attribute attribute = i.next();
//								subSchema = subSchema.add("_" + attribute.getName().getLocalPart(),
//										DataTypes.StringType, true);
//							}
//							if (count == 0) {
//								schema = schema.add(element.getName().getLocalPart(), DataTypes.StringType, true);
//							} else {
//								subSchema = subSchema.add("_VALUE", DataTypes.StringType, true);
//								// schema = schema.add(element.getName().getLocalPart(),subSchema);
//								schema = schema.add(element.getName().getLocalPart(), DataTypes.StringType, true);
//
//							}
//						}
//					}
//
//				}
//
//				if (event.isEndElement()) {
//					EndElement element = event.asEndElement();
//					if (element.getName().getLocalPart().equals("person")) {
//						break;
//					}
//				}
//			}
//			System.err.println("Schema : " + schema.toString());
//			return schema;
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (XMLStreamException e) {
//			e.printStackTrace();
//		}
//		return null;
//
//	}
//
//	public static void main(String[] args) throws FileNotFoundException, XMLStreamException, IOException {
//		StreamingXMLReader reader = new StreamingXMLReader(inferSchema(),
//				Utils.getISFromPath("/Users/akshay.sharma/Downloads/noteBookRepo/AzureDevOps-DBRepo/test.xml"),
//				"person", "people");
//		System.err.println(IteratorUtils.toList(JavaConverters.asJavaIterator(reader.get())));
//		StreamingXMLReader reader2 = new StreamingXMLReader(inferSchema(),
//				Utils.getISFromPath("/Users/akshay.sharma/Downloads/noteBookRepo/AzureDevOps-DBRepo/test.xml"),
//				"person", "people");
//		System.err.println(IteratorUtils.toList(JavaConverters.asJavaIterator(reader2.get())));
//	}
//
//}
