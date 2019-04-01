package com.stone.mr2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HdfsReducer extends TableReducer<NullWritable, Put, NullWritable> {
	
	@Override
	protected void reduce(NullWritable key, Iterable<Put> values,
			Reducer<NullWritable, Put, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
		for (Put value : values) {
			context.write(NullWritable.get(), value);
		}
	}
	
}
