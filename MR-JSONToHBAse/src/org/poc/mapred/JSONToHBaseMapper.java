package org.poc.mapred;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.poc.avro.serde.AVROSerDe.JSONToAvro;
import org.poc.mapred.helper.HBaseUtil;
import org.poc.mapred.helper.JSONUtil;
import org.poc.mapred.helper.MapRedConstants;

public class JSONToHBaseMapper
 extends Mapper<LongWritable, Text, Text, BytesWritable> 
{
	public static final Log LOG = LogFactory.getLog(JSONToHBaseMapper.class);
	static private File avroFile;

	protected void setup(Context context) throws IOException,
			InterruptedException {

		// Only once for the JVM instance
		LOG.info("Should be called only once per JVM");

		// Retrieving avro file from distributed cache using SYM(bolic)LINK
		avroFile = new File(MapRedConstants.AVRO_SCHEMA_SYMLINK);
		LOG.info("AVRO file obtained from Distributed Cache.."
				+ avroFile.getCanonicalPath());
	}

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		LOG.info("Inside Map : Record : " + value);
		String formattedJSON = JSONUtil.formatJSON(value.toString());
		LOG.info("json : " + formattedJSON);
		String jsonPK = JSONUtil.getTimestampAndMesgId(formattedJSON);
		String rowKey = HBaseUtil.getSaltingPrefix() + "|" + jsonPK; // So
																		// RowKey
																		// =
		byte[] avroBlob = JSONToAvro.jsonToAvro(formattedJSON, avroFile);
		BytesWritable bytesWritableAvroBlob = new BytesWritable(avroBlob);																// "SaltingPrefix|timestamp|messageId"
		context.write(new Text(rowKey), bytesWritableAvroBlob);
		LOG.info("One Map task successfully completed...");

	}
}
