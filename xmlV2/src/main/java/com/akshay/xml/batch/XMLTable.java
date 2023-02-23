package com.akshay.xml.batch;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

public class XMLTable implements SupportsRead {

	private final StructType schema;
	private final Map<String, String> properties;
	private Set<TableCapability> capabilities;
	private SerializableConfiguration conf;

	public XMLTable(StructType schema, Map<String, String> properties, SerializableConfiguration conf) {
		this.conf = conf;
		this.schema = schema;
		this.properties = properties;
	}

	@Override
	public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
		return new XMLScanBuilder(schema, properties, options, conf);
	}

	@Override
	public String name() {
		return "dummy_table";
	}

	@Override
	public StructType schema() {
		return schema;
	}

	@Override
	public Set<TableCapability> capabilities() {
		if (capabilities == null) {
			this.capabilities = new HashSet<>();
			capabilities.add(TableCapability.BATCH_READ);
		}
		return capabilities;
	}
}
