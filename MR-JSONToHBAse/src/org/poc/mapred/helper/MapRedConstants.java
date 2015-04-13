package org.poc.mapred.helper;


public class MapRedConstants {
	public final static String ipPath = "/hadoop-work/poc/input";
	public final static String opPath = "/hadoop-work/poc/output";
	//public final static String AVRO_SCHEMA_PATH = "hdfs://localhost:54310/hadoop-work/poc/avro/InboxJSON.avsc";
	public final static String AVRO_SCHEMA_PATH = "/hadoop-work/poc/avro/InboxJSON.avsc#InboxJSON.avsc";
	public final static String AVRO_SCHEMA_SYMLINK = "InboxJSON.avsc";
	public final static String RECORD_DELIMITER = "},";
	public final static String JSON_KEY_SEPARATOR = "|";
	public final static int HBase_REGION_SERVER_COUNT = 3;
	public final static String HBase_CONFIG_FILE = "/hadoop-work/poc/hbase-config/hbase-site.xml";
	public final static String HBase_TABLE_NAME = "msg_table";
	//public final static int SALTING_END = 4;
	public static final String HADOOP_CONFIG_CORE_SITE = "/etc/hadoop/conf/core-site.xml";
	public static final String HADOOP_CONFIG_HDFS_SITE = "/etc/hadoop/conf/hdfs-site.xml";


}