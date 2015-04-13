package org.poc.hdfs;
import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSUtil {
	
	public static Configuration getHDFSConfig() {
		Configuration conf = new Configuration();
		conf.addResource(new Path(HadoopConstants.HADOOP_CONFIG_CORE_SITE));
		conf.addResource(new Path(HadoopConstants.HADOOP_CONFIG_HDFS_SITE));
		return conf;
	}
	public static FileSystem getHDFSFileSystem(Configuration conf) throws IOException{
		FileSystem fileSystem = FileSystem.get(conf);
		return fileSystem;
	}
	public static boolean isFileExists(FileSystem fs, String filePath) throws IOException{
		if(fs.exists(new Path(filePath)))
			return true;
		return false;
	}
}
