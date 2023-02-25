package com.akshay.xml.utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class ValueConverters implements Serializable {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	public static List<Function> getConverters(StructType schema) {
		StructField[] fields = schema.fields();
		List<Function> valueConverters = new ArrayList<>(fields.length);
		for (StructField field : fields) {
			valueConverters.add(getTypes(field));
		}

		return valueConverters;
	}

	@SuppressWarnings("rawtypes")
	public static Function getTypes(StructField field) {
		Function f = null;
		if (field.dataType().equals(DataTypes.StringType)) {
			f = UTF8StringConverter;
		} else if (field.dataType().equals(DataTypes.IntegerType))
			f = IntConverter;
		else if (field.dataType().equals(DataTypes.DoubleType))
			f = DoubleConverter;
		else if (field.dataType().typeName().equals("struct")) {
			System.err.println("Here");
			System.err.println(field.dataType());
			f = StructConverter;
		}
		return f;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Object getTypesFromValue(Object field) {
		Function f = null;
		if (field.getClass().getName().equals(String.class.getName())) {
			f = UTF8StringConverter;
		} else if (field.getClass().getName().equals(int.class.getName()))
			f = IntConverter;
		else if (field.getClass().getName().equals(double.class.getName()))
			f = DoubleConverter;
		System.err.println(field.getClass());
		System.err.print(String.class);
		return f.apply(field);
	}

	public static Function<String, UTF8String> UTF8StringConverter = UTF8String::fromString;
	public static Function<String, Double> DoubleConverter = value -> value == null ? null : Double.parseDouble(value);
	public static Function<String, Integer> IntConverter = value -> value == null ? null : Integer.parseInt(value);
	public static Function<InternalRow, InternalRow> StructConverter = value -> value == null ? null : value;

}
