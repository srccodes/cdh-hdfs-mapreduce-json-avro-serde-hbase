package org.poc.mapred.helper;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseUtil {
	public static int getSaltingPrefix() {
		return (((int) (Math.random() * 10)) % MapRedConstants.HBase_REGION_SERVER_COUNT);
	}

	public static Configuration getHBaseConfig() {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(MapRedConstants.HBase_CONFIG_FILE));
		conf.set("hbase.master", "localhost:60000");
		return conf;
	}
	private static void generateTimestamps() {
		for(int i=1; i <=6; i++) {
			Timestamp ts = new Timestamp(new GregorianCalendar(2014, Calendar.JANUARY,i, 11, 10, 15).getTimeInMillis());
			System.out.println(ts);
		}
	}
	public static void main(String[] args) throws IOException {
		for (int i = 0; i < 10; i++) {
			System.out.println(((int) (Math.random() * 10))
					% MapRedConstants.HBase_REGION_SERVER_COUNT);
		}
		HBaseAdmin admin = new HBaseAdmin(getHBaseConfig());
		System.out.println(admin.getTableNames()[0]);
		generateTimestamps();
	}
	

}
