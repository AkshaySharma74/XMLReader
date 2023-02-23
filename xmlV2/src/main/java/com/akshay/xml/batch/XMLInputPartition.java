package com.akshay.xml.batch;

import org.apache.spark.sql.connector.read.InputPartition;

public class XMLInputPartition implements InputPartition {

	private static final long serialVersionUID = 1L;
	
	private String xmlPath;
	
	public XMLInputPartition(String path) {
		this.xmlPath = path;
	}

	@Override
	public String[] preferredLocations() {
		System.err.println("Input Partitions");
		System.err.println(new String[0]);
		return new String[0];
	}
	
	public String getPath() {
		return this.xmlPath;
	}
	
	@Override
	public String toString() {
		return "XMLInputPartition : "+this.xmlPath;
	}
}
