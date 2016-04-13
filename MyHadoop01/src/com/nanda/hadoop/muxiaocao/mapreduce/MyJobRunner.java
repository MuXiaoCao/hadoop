package com.nanda.hadoop.muxiaocao.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用来描述一个特定的作业
 * 具体描述：该作业使用具体哪个类作为map，哪个作为reduce，
 * 还可以指定该作业要处理的数据所在路径、
 * 还可以指定该作业输出的结果放到哪个路径
 * @author muxiaocao
 *
 */
public class MyJobRunner {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			
			Job wcJob = Job.getInstance(conf);
			
			// 设置整个job所用的类属于哪个jar包
			wcJob.setJarByClass(MyJobRunner.class);
			
			// 指定map和reduce具体是哪个类
			wcJob.setMapperClass(MyMap.class);
			wcJob.setReducerClass(MyReduce.class);
			
			// 指定reduce的key、value的类型
			wcJob.setOutputKeyClass(Text.class);
			wcJob.setOutputValueClass(LongWritable.class);
			
			// 指定map的key、value类型
			wcJob.setMapOutputKeyClass(Text.class);
			wcJob.setMapOutputValueClass(LongWritable.class);
			
			// 指定数据源
			FileInputFormat.setInputPaths(wcJob,new Path("/home/muxiaocao/myFile/srcdata"));
			
			// 指定输出文件
			FileOutputFormat.setOutputPath(wcJob, new Path("/home/muxiaocao/myFile/output"));
		
			// 将job提交给集群运行
			wcJob.waitForCompletion(true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
