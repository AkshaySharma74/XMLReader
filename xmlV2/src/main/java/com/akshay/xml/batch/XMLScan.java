package com.akshay.xml.batch;

import java.util.Map;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

public class XMLScan implements Scan {
	private final StructType schema;
	private final Map<String, String> properties;
	private final CaseInsensitiveStringMap options;
	private SerializableConfiguration conf;

	public XMLScan(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options,
			SerializableConfiguration conf) {

		this.schema = schema;
		this.properties = properties;
		this.options = options;
		this.conf = conf;
	}

	@Override
	public StructType readSchema() {
		return schema;
	}

	@Override
	public String description() {
		return "xml_scan";
	}

	@Override
	public Batch toBatch() {
		XMLBatch test = new XMLBatch(schema, properties, options, conf);
		return test;
	}
}
