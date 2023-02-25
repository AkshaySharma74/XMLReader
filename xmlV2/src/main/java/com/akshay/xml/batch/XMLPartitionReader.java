package com.akshay.xml.batch;

import java.io.IOException;
import java.io.InputStream;
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
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import com.akshay.xml.utilities.Utils;
import com.akshay.xml.utilities.ValueConverters;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

@SuppressWarnings("rawtypes")
public class XMLPartitionReader implements PartitionReader<InternalRow> {

	// private final String fileName;
	private XMLEventReader reader;
	private List<Function> valueConverters;
	private String rowTag;
	private Map<String, String> schema;
	private boolean hasManualNext = true;
	private SerializableConfiguration conf;
	private XMLInputPartition xmlInputPartition;

	public XMLPartitionReader(XMLInputPartition xmlInputPartition, StructType schema, String fileName, String rowTag,
			String rootTag, SerializableConfiguration conf) throws XMLStreamException, IOException {
		// this.fileName = fileName;
		this.valueConverters = ValueConverters.getConverters(schema);
		this.rowTag = rowTag;
		this.schema = getSchemaMap(schema);
		this.conf = conf;
		this.xmlInputPartition = xmlInputPartition;
		this.createXMLReader();
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
		InputStream stream = Utils.getISFromPath(this.xmlInputPartition.getPath(), conf);
		reader = inputFactory.createXMLEventReader(stream);
		reader.nextEvent();
	}

	@Override
	public void close() throws IOException {
		try {
			reader.close();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean next() throws IOException {
		if (this.hasManualNext) {
			return reader.hasNext();
		} else {
			return this.hasManualNext;
		}
	}

	@SuppressWarnings("unchecked")
	public List<Object> getConvertedValues(XMLEvent event, int loopValue) throws XMLStreamException {
		List<Object> convertedValues = new ArrayList<>();
		StartElement element = event.asStartElement();
		event = reader.nextEvent();
		int cnt = 0;
		if (event.isCharacters() && this.schema.get(element.getName().getLocalPart()) != null) {
			String elementOne = event.asCharacters().getData();
			if (elementOne.replaceAll("\n", "").trim().equals("")) {
				elementOne = null;
			}

			Iterator<Attribute> i = element.getAttributes();

			List<Object> ll = new ArrayList<>();
			while (i.hasNext()) {
				Attribute attribute = i.next();
				ll.add(ValueConverters.getTypesFromValue(attribute.getValue().toString()));
				cnt += 1;
			}
			if (cnt == 0) {
				convertedValues.add(valueConverters.get(loopValue).apply(elementOne));
			} else {
				ll.add(ValueConverters.getTypesFromValue(elementOne.toString()));
				InternalRow inner = InternalRow.apply(JavaConverters.asScalaBuffer(ll).toList());
				convertedValues.add(valueConverters.get(loopValue).apply(inner));
			}

			return convertedValues;

		}
		return new ArrayList<>();

	}

	public List<Object> getValues() throws XMLStreamException, IOException {

		List<Object> convertedValues = new ArrayList<>();

		if (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();
			while (!(event.isStartElement() && this.schema.get(getText(event)) != null)) {
				if (this.schema.get(getText(event)) != null) {
					break;
				} else if (event.isEndDocument()) {
					reader.close();
					break;
				} else {
					event = reader.nextEvent();
				}
			}
			if (event.isStartElement()) {
				StartElement element = event.asStartElement();
				if (event.isStartElement() && this.schema.get(element.getName().getLocalPart()) != null) {
					event = addToValues(convertedValues, event);
				}
			}

			if (this.schema.get(getText(event)) != null || getText(event).equals(this.rowTag)) {
				// DO NOTHING

			} else {
				this.hasManualNext = false;
				this.close();
			}

		}
		return convertedValues;

	}

	private XMLEvent addToValues(List<Object> convertedValues, XMLEvent event2) throws XMLStreamException {
		XMLEvent event = event2;
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
		return event;
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

	@Override
	public InternalRow get() {

		List<Object> convertedValues = new ArrayList<>();

		try {
			convertedValues.addAll(getValues());
		} catch (XMLStreamException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return InternalRow.apply(JavaConversions.asScalaBuffer(convertedValues).toList());

	}

}
