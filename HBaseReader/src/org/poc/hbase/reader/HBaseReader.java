package org.poc.hbase.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.poc.avro.serde.AVROSerDe.AvroToJSON;
import org.poc.hbase.util.HBaseConstants;

public class HBaseReader {
	public static final Log LOG = LogFactory.getLog(HBaseReader.class);
	public static Configuration getHBaseConfig() {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(HBaseConstants.HBase_CONFIG_FILE));
		conf.set("hbase.master", "localhost:60000");
		return conf;
	}
	/*Brief Description of FuzzyRowFilter
	 * ----------------------------------
	 * Filters data based on fuzzy row key. Performs fast-forwards during scanning. 
	 * It takes pairs (row key, fuzzy info) to match row keys. 
	 * Where fuzzy info is a byte array with 0 or 1 as its values:
	 * 0 - means that this byte in provided row key is fixed, i.e. row key's byte at same position must match
     * 1 - means that this byte in provided row key is NOT fixed, i.e. row key's byte at this position can be different from the one in provided row key

		Example: Let's assume row key format is userId_actionId_year_month. 
		Length of userId is fixed and is 4, length of actionId is 2 and year and month are 4 and 2 bytes long respectively. 
		Let's assume that we need to fetch all users that performed certain action (encoded as "99") in Jan of any year. 
		Then the pair (row key, fuzzy info) would be the following: 
		row key = "????_99_????_01" (one can use any value instead of "?") 
		fuzzy info = "\x01\x01\x01\x01\x00\x00\x00\x00\x01\x01\x01\x01\x00\x00\x00" 
		I.e. fuzzy info tells the matching mask is "????_99_????_01", where at ? can be any value. 
	 */
	
	/*
	 * In our case : TARTGET QUERY :get all the messages between 2nd Jan, 2014 to 4th Jan, 2014
	 * Row Key : saltingprefix|timestamp|mesgid
	 * e.g. Row Key : 1|2014-01-05 11:10:15.0|005
	 * 
	 * So Row Key pattern : ?|2014-01-?? ??:??:??.?|???
	 */
	public static List<String> getDataUsingFuzzyRowFilter() throws IOException {
		Pair<byte[], byte[]> jan2 = new Pair<byte[], byte[]>(Bytes.toBytes("?|2014-01-02 ??:??:??.?|???"),
			    new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,1,1,1,1,1,1,1,1,1,1,0,1,1,1});
		Pair<byte[], byte[]> jan3 = new Pair<byte[], byte[]>(Bytes.toBytes("?|2014-01-03 ??:??:??.?|???"),
			    new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,1,1,1,1,1,1,1,1,1,1,0,1,1,1});
		Pair<byte[], byte[]> jan4 = new Pair<byte[], byte[]>(Bytes.toBytes("?|2014-01-04 ??:??:??.?|???"),
			    new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,1,1,1,1,1,1,1,1,1,1,0,1,1,1});
		
		List<Pair<byte[], byte[]>> fuzzyFilterKeys = new ArrayList<Pair<byte[], byte[]>>();
		fuzzyFilterKeys.add(jan2);
		fuzzyFilterKeys.add(jan3);
		fuzzyFilterKeys.add(jan4);
		
		FuzzyRowFilter fwf = new FuzzyRowFilter(fuzzyFilterKeys);
		HTable hTable = new HTable(getHBaseConfig(), HBaseConstants.HBase_TABLE_NAME);
		
		Scan scan = new Scan();
		scan.setFilter(fwf);
		ResultScanner scanner = hTable.getScanner(scan);
		System.out.println("Scanning table... ");
	    for (Result result : scanner) {
	        //System.out.println("getRow:"+Bytes.toString(result.getRow()));
	        for (KeyValue kv : result.raw()) {
	            //System.out.println("Family - "+Bytes.toString(kv.getFamily()));
	            //System.out.println("Qualifier - "+Bytes.toString(kv.getQualifier() ));
	        	System.out.println("kv:"+kv +"\nKey: " + Bytes.toString(kv.getRow())  + "\nValue: " +AvroToJSON.avroToJson(kv.getValue()));
	        	System.out.println("--------------------------------------------------------------");
	        }
	    }   
		return null;
		
	}
	public static void main(String[] args) {
		try {
			getDataUsingFuzzyRowFilter();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
