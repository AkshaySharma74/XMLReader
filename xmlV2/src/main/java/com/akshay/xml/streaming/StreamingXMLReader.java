package com.akshay.xml.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.akshay.xml.utilities.ValueConverters;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

@SuppressWarnings("rawtypes")
public class StreamingXMLReader implements Serializable {
	private static final long serialVersionUID = 1L;
	private final InputStream is;
	private XMLEventReader reader;
	private List<Function> valueConverters;
	private Map<String, String> schema;

	public StreamingXMLReader(StructType schema, InputStream is, String rowTag, String rootTag)
			throws XMLStreamException, IOException {
		this.is = is;
		this.valueConverters = ValueConverters.getConverters(schema);
		this.schema = getSchemaMap(schema);

	}

	public Map<String, String> getSchemaMap(StructType schema) {
		Map<String, String> schemaMap = new HashMap<>();
		for (StructField iterable_element : schema.fields()) {
			schemaMap.put(iterable_element.name(), iterable_element.dataType().toString());
		}
		return schemaMap;
	}

	public void createXMLReader() throws XMLStreamException, IOException {
		XMLInputFactory inputFactory = XMLInputFactory.newFactory();
		inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
		reader = inputFactory.createXMLEventReader(this.is);
		reader.nextEvent();
	}

	public void close() throws IOException {
		try {
			reader.close();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public List<Object> getConvertedValues(XMLEvent event, int loopValue) throws XMLStreamException {
		List<Object> convertedValues = new ArrayList<>();
		StartElement element = event.asStartElement();
		event = reader.nextEvent();
		int cnt = 0;
		Map<String, Map<String, String>> mainMap = new HashMap<>();
		if (event.isCharacters() && this.schema.get(element.getName().getLocalPart()) != null) {
			String elementOne = event.asCharacters().getData();
			if (elementOne.replaceAll("\n", "").trim().equals("")) {
				elementOne = null;
			}

			Iterator<Attribute> i = element.getAttributes();

			Map<String, String> valueMap = new HashMap<>();
			while (i.hasNext()) {
				Attribute attribute = i.next();
				valueMap.put("_" + attribute.getName().getLocalPart(), attribute.getValue());
				cnt += 1;
			}
			if (cnt == 0) {
				convertedValues.add(valueConverters.get(loopValue).apply(elementOne));
			} else {
				valueMap.put("_VALUE", elementOne);
				mainMap.put(element.getName().getLocalPart(), valueMap);
				convertedValues.add(valueConverters.get(loopValue).apply(mainMap.toString()));

			}

			return convertedValues;

		}
		return new ArrayList<>();

	}

	public List<Object> getValues() throws XMLStreamException, IOException {

		List<Object> convertedValues = new ArrayList<>();

		if (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();

			if (event.isStartElement()) {
				StartElement element = event.asStartElement();
				if (event.isStartElement() && this.schema.get(element.getName().getLocalPart()) != null) {
					convertedValues = addToValues(event);
				}
			}

		}
		return convertedValues;

	}

	private List<Object> addToValues(XMLEvent event2) throws XMLStreamException {
		XMLEvent event = event2;
		List<Object> convertedValues = new ArrayList<>();
		for (int i = 0; i < this.schema.keySet().size(); i++) {
			if (event.isStartElement()) {
				convertedValues.addAll(getConvertedValues(event, i));
				while (!reader.peek().isStartElement()) {
					if (reader.peek().isEndDocument()) {
						break;
					}
					event = reader.nextEvent();
				}
				event = reader.nextEvent();

			}

		}
		return convertedValues;
	}

	public String getText(XMLEvent event) {

		String text = "";
		if (event.isStartElement()) {
			StartElement element = (StartElement) event;
			text = element.getName().getLocalPart();
		}
		if (event.isEndElement()) {
			EndElement element = (EndElement) event;
			text = element.getName().getLocalPart();
		}
		if (event.isCharacters()) {
			Characters characters = (Characters) event;
			text = characters.getData();
		}

		return text;

	}

	public scala.collection.Iterator<InternalRow> get() throws XMLStreamException, IOException {
		this.createXMLReader();
		List<InternalRow> rows = new ArrayList<>();
		while (reader.hasNext()) {
			List<Object> convertedValues = new ArrayList<>();

			try {
				convertedValues.addAll(getValues());
			} catch (XMLStreamException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			//System.err.println(convertedValues);
			if (!convertedValues.isEmpty()) {
				rows.add(InternalRow.
						apply(JavaConversions.asScalaBuffer(convertedValues).toList()));
			}
		}

		return JavaConverters.asScalaIteratorConverter(rows.iterator()).asScala();

	}
}
