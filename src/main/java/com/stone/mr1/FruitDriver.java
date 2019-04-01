package com.stone.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver extends Configuration implements Tool {
	
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
		//获取任务对象
		Job job = Job.getInstance(conf);
		
		//指定Driver类
		job.setJarByClass(FruitDriver.class);
		
		//指定Mapper
		TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), FruitMapper.class, ImmutableBytesWritable.class, Put.class, job);
		
		//指定Reducer
		TableMapReduceUtil.initTableReducerJob("fruit_mr", FruitReducer.class, job);
		
		//提交
		boolean b = job.waitForCompletion(true);
		
		return b ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.30.60.62");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		int i = ToolRunner.run(conf, new FruitDriver(), args);
		
		System.exit(i);
	}

}
