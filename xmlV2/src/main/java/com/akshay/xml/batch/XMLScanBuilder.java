package com.akshay.xml.batch;

import java.util.Map;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

public class XMLScanBuilder implements ScanBuilder {

	private final StructType schema;
	private final Map<String, String> properties;
	private final CaseInsensitiveStringMap options;
	private SerializableConfiguration conf;

	public XMLScanBuilder(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options,
			SerializableConfiguration conf) {

		this.schema = schema;
		this.properties = properties;
		this.options = options;
		this.conf = conf;
	}

	@Override
	public Scan build() {
		return new XMLScan(schema, properties, options, conf);
	}
}
