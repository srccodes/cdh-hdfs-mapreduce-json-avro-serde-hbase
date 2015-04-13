package org.poc.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class JSONToHBaseReducer extends
		TableReducer<Text, BytesWritable, NullWritable> {
	public static final Log LOG = LogFactory.getLog(JSONToHBaseMapper.class);
	public void reduce(Text rowKey, Iterable<BytesWritable> avroBlobs, Context context)
			throws IOException, InterruptedException {
		try {
			System.out.println("Rowkey : " + rowKey);
			byte[] bRowKey = Bytes.toBytes(rowKey.toString());
			byte[] avroBlob = ((BytesWritable)avroBlobs.iterator().next()).getBytes();

			Put put = new Put(bRowKey);
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("c"), avroBlob);

			//ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bRowKey);
			// Writing to HBase table
			NullWritable opKey = null;
			context.write(opKey, put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
