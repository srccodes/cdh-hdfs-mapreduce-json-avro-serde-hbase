package org.poc.runner;

import org.poc.hdfs.FileLoader;

public class HDFSFileLoaderRunner {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String sourceLocalPathJSON = "/home/hduser/Desktop/Docs/inbox.json";
		String targetHDFSPathJSON = "/hadoop-work/poc/input/inbox.json";
		
		String sourceLocalPathAVROSchema = "/home/hduser/hadoop-workspace/MR-JSONToHBAse/resource/InboxJSON.avsc";
		String targetHDFSPathAVROSchema = "/hadoop-work/poc/avro/InboxJSON.avsc";
		
		String sourceLocalPathHBaseConfig = "/etc/hbase/conf/hbase-site.xml";
		String targetHDFSPathHBaseConfig = "/hadoop-work/poc/hbase-config/hbase-site.xml";
		
		try {
			FileLoader.copyFromLocal(sourceLocalPathJSON, targetHDFSPathJSON);
			FileLoader.copyFromLocal(sourceLocalPathAVROSchema, targetHDFSPathAVROSchema);
			FileLoader.copyFromLocal(sourceLocalPathHBaseConfig, targetHDFSPathHBaseConfig);
			System.out.println("copyFromLocal completed successfully..");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
