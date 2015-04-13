package org.poc.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileLoader {
	
	public static void copyFromLocal(String sourceLocalPath, String targetHDFSPath) throws Exception{
		Configuration conf = HDFSUtil.getHDFSConfig();
		FileSystem fs = HDFSUtil.getHDFSFileSystem(conf);
		boolean isTargetFileExists = HDFSUtil.isFileExists(fs, targetHDFSPath);
		if(!isTargetFileExists) {
			// Create a new file and write data to it.
		    FSDataOutputStream out = fs.create(new Path(targetHDFSPath));
		    InputStream in = new BufferedInputStream(new FileInputStream(
		        new File(sourceLocalPath)));
		    IOUtils.copy(in, out);
		    // Close all the file descriptors
		    in.close();
		    out.close();
		    fs.close();
		} else {
			System.out.println("HDFS File already exists");
		}
		
	}

}
