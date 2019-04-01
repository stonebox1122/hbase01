package com.stone.mr2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HdfsMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Put>.Context context)
			throws IOException, InterruptedException {
		//获取一行数据
		String line = value.toString();
		
		//切割
		String[] split = line.split("\t");
		
		//封装Put对象
		Put put = new Put(Bytes.toBytes(split[0]));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(split[2]));
		
		//写出
		context.write(NullWritable.get(), put);
	}

}
