package com.akshay.xml.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

public class Utils {

	public static InputStream getISFromPath(String v1path, SerializableConfiguration conf)
			throws MalformedURLException, IOException {
		if (v1path.startsWith("/")) {
			InputStream is = new FileInputStream(v1path);
			return is;
		}
		Path p = new Path(v1path);
		FileSystem fs = FileSystem.get(p.toUri(), conf.value());
		InputStream in = fs.open(p).getWrappedStream();
		return in;
	}

	public static boolean isDir(String v1path, SerializableConfiguration conf) throws IOException {
		Path p = new Path(v1path);
		FileSystem fs = FileSystem.get(p.toUri(), conf.value());
		return fs.isDirectory(p);
	}

	public static InputStream getISFromPath(String v1path) throws MalformedURLException, IOException {
		InputStream in = new FileInputStream(v1path);
		return in;
	}

	public static List<String> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
		List<String> fileList = new ArrayList<String>();
		FileStatus[] fileStatus = fs.listStatus(filePath);
		for (FileStatus fileStat : fileStatus) {
			if (fileStat.isDirectory()) {
				fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
			} else {
				fileList.add(fileStat.getPath().toString());
			}
		}
		return fileList;
	}

	public static InputStream getSchemaISFromPath(String v1path, SerializableConfiguration conf)
			throws MalformedURLException, IOException {
		Path p = new Path(v1path);

		if (v1path.startsWith("/")) {
			InputStream is;
			File f = new File(v1path);
			if (f.isDirectory()) {
				is = new FileInputStream(f.listFiles()[0]);
			} else {
				is = new FileInputStream(v1path);
			}
			return is;
		}
		FileSystem fs = FileSystem.get(p.toUri(), conf.value());

		Path pathValue = p;

		if (fs.isDirectory(p)) {
			System.err.println(fs.getScheme());
			System.err.println(fs.isFile(p));
			System.err.println(fs.getUri().toString());
			List<String> files = getAllFilePath(p, fs);
			System.err.println(files);
			String fileName = files.get(0);
			if (fileName.startsWith("/")) {
				fileName = "file:" + fileName;
			}
			pathValue = new Path(fileName);

		} else {

		}

		InputStream in = fs.open(pathValue).getWrappedStream();
		return in;
	}

	public static FileSystem getFsFromPath(String v1path, SerializableConfiguration conf) throws IOException {
		Path p = new Path(v1path);
		FileSystem fs = FileSystem.get(p.toUri(), conf.value());
		return fs;
	}

	public static boolean isLocalDir(String v1path) {
		File f = new File(v1path);
		return f.isDirectory();
	}

	public static List<String> getLocalFiles(String v1path) {
		File f = new File(v1path);
		if (f.isDirectory()) {
			return Arrays.asList(f.listFiles()).stream().map(File::getAbsolutePath).collect(Collectors.toList());
		}
		return new ArrayList<>();
	}

}
