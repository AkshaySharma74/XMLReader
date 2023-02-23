package com.akshay.xml.streaming;

import java.io.IOException;
import java.io.Serializable;

import javax.xml.stream.XMLStreamException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;

import com.akshay.xml.utilities.Utils;

import scala.Function1;
import scala.collection.Iterator;

public class Function implements Function1<PartitionedFile, scala.collection.Iterator<InternalRow>>, Serializable {

	StructType schema;
	String rootTag;
	String rowTag;
	SerializableConfiguration conf;

	public Function(StructType schema, String rootTag, String rowTag, SerializableConfiguration conf) {

		this.schema = schema;
		this.rootTag = rootTag;
		this.rowTag = rowTag;
		this.conf = conf;
	}

	private static final long serialVersionUID = 1L;

	public static java.util.function.Function<String, UTF8String> UTF8StringConverter = UTF8String::fromString;

	@Override
	public Iterator<InternalRow> apply(PartitionedFile v1) {

		try {

			StreamingXMLReader reader = new StreamingXMLReader(schema, Utils.getISFromPath(v1.filePath(), conf), rowTag,
					rootTag);
			return reader.get();
		} catch (XMLStreamException | IOException e) {
			e.printStackTrace();
		}

		return null;
	}

}
