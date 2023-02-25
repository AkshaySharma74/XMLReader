package com.akshay.xml.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

import com.akshay.xml.batch.XMLTable;
import com.akshay.xml.streaming.Function;
import com.akshay.xml.utilities.Utils;

import scala.Function1;
import scala.Option;
import scala.collection.JavaConversions;



public class DefaultSource implements FileFormat, TableProvider {
	
	
	

	public DefaultSource() {
	}

	@Override
	@SuppressWarnings("unchecked")
	public StructType inferSchema(CaseInsensitiveStringMap options) {
		SerializableConfiguration conf = new SerializableConfiguration(SparkSession.active().sparkContext().hadoopConfiguration());
		InputStream inputStream;
		try {
			inputStream = Utils.getSchemaISFromPath(options.get("path"),conf);
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			XMLEventReader reader = inputFactory.createXMLEventReader(inputStream);
			StructType schema = new StructType();
			boolean schemaFlag = false;
			while (reader.hasNext()) {
				XMLEvent event = (XMLEvent) reader.next();

				if (event.isStartElement()) {
					StartElement element = event.asStartElement();
					event = (XMLEvent) reader.next();
					if (element.getName().getLocalPart().equals(options.get("rowTag"))) {
						schemaFlag = true;
						continue;
					}
					if (schemaFlag) {
						int count = 0;
						if (event.isCharacters()) {
							Iterator<Attribute> i = element.getAttributes();
							StructType subSchema = new StructType();
							while (i.hasNext()) {

								count += 1;
								Attribute attribute = i.next();
								subSchema = subSchema.add("_" + attribute.getName().getLocalPart(),
										DataTypes.StringType, true);
							}
							if (count == 0) {
								schema = schema.add(element.getName().getLocalPart(), DataTypes.StringType, true);
							} else {
								subSchema = subSchema.add("_VALUE", DataTypes.StringType, true);
								schema = schema.add(element.getName().getLocalPart(), subSchema);
								//schema = schema.add(element.getName().getLocalPart(), DataTypes.StringType, true);

							}
						}
					}

				}

				if (event.isEndElement()) {
					EndElement element = event.asEndElement();
					if (element.getName().getLocalPart().equals(options.get("rowTag"))) {
						break;
					}
				}
			}
			System.err.println("Schema : " + schema.toString());
			return schema;
		} catch (XMLStreamException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	@Override
	public OutputWriterFactory prepareWrite(SparkSession sparkSession, Job job,
			scala.collection.immutable.Map<String, String> options, StructType dataSchema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> buildReader(SparkSession sparkSession,
			StructType dataSchema, StructType partitionSchema, StructType requiredSchema,
			scala.collection.Seq<Filter> filters, scala.collection.immutable.Map<String, String> options,
			Configuration hadoopConf) {

		Map<String, String> scalaMap = JavaConversions.mapAsJavaMap(options);
		Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> testFunction = new Function(dataSchema,
				scalaMap.get("rootTag"), scalaMap.get("rowTag"),new SerializableConfiguration(hadoopConf));

		return testFunction;
	}

	@Override
	public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
		Configuration conf = SparkSession.active().sparkContext().hadoopConfiguration();
		XMLTable tab = new XMLTable(schema, properties,new SerializableConfiguration(conf));
		return tab;
	}

	@Override
	public Option<StructType> inferSchema(SparkSession sparkSession,
			scala.collection.immutable.Map<String, String> options, scala.collection.Seq<FileStatus> files) {
		Map<String, String> scalaMap = JavaConversions.mapAsJavaMap(options);

		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap(scalaMap);
		return Option.apply(this.inferSchema(map));
	}

}
