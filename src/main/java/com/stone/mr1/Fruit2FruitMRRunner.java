package com.stone.mr1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2FruitMRRunner extends Configuration implements Tool {

    private Configuration conf = null;

    public Configuration getConf() {
        return this.conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public int run(String[] args) throws Exception {

        // 创建Job任务
        Job job = Job.getInstance(this.getConf());

        // 指定Runner类
        job.setJarByClass(Fruit2FruitMRRunner.class);

        // 配置Job
        Scan scan = new Scan();
        // scan.setCacheBlocks(false);
        // scan.setCaching(500);

        // 指定Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit",                        // 数据源的表名
                scan,                          // scan扫描控制器
                ReadFruitFromHBaseMapper.class,          // 设置Mapper类
                ImmutableBytesWritable.class,   // 设置Mapper输出key类型
                Put.class,                      // 设置Mapper输出value值类型
                job                             // 设置给哪个JOB
        );

        // 指定Reducer
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr", 
                WriteFruitMRReducer.class, 
                job);

        // 设置Reduce数量，最少1个
        job.setNumReduceTasks(1);

        // 提交
        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new Fruit2FruitMRRunner(), args);
        System.exit(status);
    }

}