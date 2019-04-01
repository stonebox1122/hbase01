package com.stone.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HdfsDriver extends Configuration implements Tool {
	
	private Configuration conf = null;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		//获取Job对象
		Job job = Job.getInstance(conf);
		
		//设置主类
		job.setJarByClass(HdfsDriver.class);
		
		//设置Mapper
		job.setMapperClass(HdfsMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		//设置reducer
		TableMapReduceUtil.initTableReducerJob("fruit2", HdfsReducer.class, job);
		
		//设置输入路径
		FileInputFormat.setInputPaths(job, args[0]);
		
		//提交
		boolean result = job.waitForCompletion(true);
		
		return result ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();
		int i = ToolRunner.run(configuration, new HdfsDriver(), args);
		System.exit(i);
	}

}
