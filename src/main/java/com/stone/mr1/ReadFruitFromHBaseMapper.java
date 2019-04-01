package com.stone.mr1;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadFruitFromHBaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // 将fruit的name和color提取出来，相当于将每一行数据读取出来放入到Put对象中。
        Cell[] cells = value.rawCells();

        // 创建Put对象
        Put put = new Put(key.get());

        for (Cell cell : cells) {
            // 添加/克隆列族:info
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                // 添加/克隆列：name
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    // 将该列cell加入到Put对象中
                    put.add(cell);
                    // 添加/克隆列:color
                } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    // 向该列cell加入到Put对象中
                    put.add(cell);
                }
            }
        }

        // 将从fruit读取到的每行数据写入到context中作为map的输出
        context.write(key, put);
    }
}