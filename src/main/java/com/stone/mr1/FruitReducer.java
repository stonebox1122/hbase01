package com.stone.mr1;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FruitReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
	
	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> values,
			Reducer<ImmutableBytesWritable, Put, NullWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		//遍历写出
		for (Put value : values) {
			context.write(NullWritable.get(), value);
		}
	}

}
