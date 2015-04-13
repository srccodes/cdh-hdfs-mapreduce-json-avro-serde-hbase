package org.poc.runner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.poc.mapred.JSONToHBaseMapper;
import org.poc.mapred.JSONToHBaseReducer;
import org.poc.mapred.helper.HBaseUtil;
import org.poc.mapred.helper.MapRedConstants;

public class JSONToHBaseRunner extends Configured implements Tool{
	
	public static final Log LOG = LogFactory.getLog(JSONToHBaseRunner.class);
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		args = new String[2];
		args[0] = MapRedConstants.ipPath;
		args[1] = MapRedConstants.opPath;
		int res = ToolRunner.run(new Configuration(), new JSONToHBaseRunner(), args);
		System.exit(res);
	}
	public int run(String[] args) throws Exception {
		// getting configuration object and setting job name
		System.out.println("Run method started yet again.....");
		//Configuration conf = getConf();
		Configuration conf = HBaseUtil.getHBaseConfig();
		
		conf.set("textinputformat.record.delimiter",MapRedConstants.RECORD_DELIMITER);
		//DistributedCache.addCacheFile(new URI(URLEncoder.encode(MapRedConstants.AVRO_SCHEMA_PATH, "UTF-8")), conf);
		conf.set("mapred.cache.files", MapRedConstants.AVRO_SCHEMA_PATH);
		conf.set(TableOutputFormat.OUTPUT_TABLE, MapRedConstants.HBase_TABLE_NAME);
		DistributedCache.createSymlink(conf);
		Job job = new Job(conf, "JSONToHBaseMapper hadoop-0.20");
		
		
		// setting the class names
		job.setJarByClass(JSONToHBaseRunner.class);
		job.setMapperClass(JSONToHBaseMapper.class);
		job.setReducerClass(JSONToHBaseReducer.class);
		
		// setting the output data type classes -- appropriate to insert data in HBase
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);


		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
 
		// to accept the hdfs input and output dir at run time
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		/*
		// Connecting to HBase and target table
		// Loading HBase Configuration
		Configuration hBaseConf = HBaseUtil.getHBaseConfig();
		HBaseAdmin admin = new HBaseAdmin(hBaseConf);
		LOG.info(admin.getTableNames()[0]);
		
		HTable hTable = new HTable(hBaseConf, MapRedConstants.HBase_TABLE_NAME);
		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		TableMapReduceUtil.initTableReducerJob(
				MapRedConstants.HBase_TABLE_NAME, // output table
				JSONToHBaseReducer.class, // reducer class
				job);
		*/
		// Adding avro schema file to the distributed cache
		//job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
