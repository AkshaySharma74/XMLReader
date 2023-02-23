package com.akshay.xml.batch;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

public class XMLPartitionReaderFactory implements PartitionReaderFactory {

	private static final long serialVersionUID = 1L;
	private final StructType schema;
	private final String filePath;
	private final String rowTag;
	private final String rootTag;
	private SerializableConfiguration conf;

	public XMLPartitionReaderFactory(StructType schema, String fileName, String rowTag, String rootTag,
			SerializableConfiguration conf) {
		this.schema = schema;
		this.filePath = fileName;
		this.rowTag = rowTag;
		this.rootTag = rootTag;
		this.conf = conf;
	}

	@Override
	public PartitionReader<InternalRow> createReader(InputPartition partition) {
		try {
			XMLPartitionReader test = new XMLPartitionReader((XMLInputPartition) partition, schema, filePath, rowTag,
					rootTag, conf);
			return test;
		} catch (XMLStreamException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

}
