package com.akshay.xml.batch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

import com.akshay.xml.utilities.Utils;

public class XMLBatch implements Batch {

	private final StructType schema;
	private final String filename;
	private final String rowTag;
	private final String rootTag;
	private SerializableConfiguration conf;

	public XMLBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options,
			SerializableConfiguration conf) {

		this.schema = schema;
		this.filename = options.get("path");
		this.rowTag = options.get("rowTag");
		this.rootTag = options.get("rootTag");
		this.conf = conf;
	}

	@Override
	public InputPartition[] planInputPartitions() {
		//return new InputPartition[] { new XMLInputPartition() };
		try {
			return createPartitions();
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new InputPartition[] { new XMLInputPartition("test") };
	}

	@Override
	public PartitionReaderFactory createReaderFactory() {
		return new XMLPartitionReaderFactory(schema, filename, rowTag, rootTag, conf);
	}
	
	public InputPartition[] createPartitions() throws FileNotFoundException, IllegalArgumentException, IOException {
		InputPartition[] partitions;
		System.err.println("T/F : ");
		System.err.println(Utils.isDir(this.filename,conf));
		System.err.println(Utils.isLocalDir(this.filename));
		if(Utils.isDir(this.filename,conf)) {
			List<String> paths = Utils.getAllFilePath(new Path(this.filename), Utils.getFsFromPath(this.filename,conf));
			System.err.println("Paths : "+paths.toString());
			partitions = new InputPartition[paths.size()];
			for(int i= 0; i< partitions.length;i++) {
				partitions[i] = new XMLInputPartition(paths.get(i));
			}
		}
		else if(Utils.isLocalDir(this.filename)) {
			List<String> paths = Utils.getLocalFiles(this.filename);
			partitions = new InputPartition[paths.size()];
			for(int i= 0; i< partitions.length;i++) {
				partitions[i] = new XMLInputPartition(paths.get(i));
			}
		}
		else {
			partitions = new InputPartition[] {new XMLInputPartition(this.filename)};
		}
		
		for (InputPartition inputPartition : partitions) {
			System.err.println(XMLInputPartition.class.cast(inputPartition).toString());
		}
		
		return partitions;
	}
}
