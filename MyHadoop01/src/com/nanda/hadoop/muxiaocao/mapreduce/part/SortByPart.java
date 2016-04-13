package com.nanda.hadoop.muxiaocao.mapreduce.part;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 按照省份分组统计流量信息 1. 需要重写partitioner中的分组方法 2. 定义reduce并发数
 * 
 * @author muxiaocao
 * 
 */
public class SortByPart extends Configured implements Tool{

	public static class SortByPartMapper extends
			Mapper<LongWritable, Text, Text, FlowBean> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = StringUtils.split(line, "\t");
			context.write(
					new Text(data[0]),
					new FlowBean(data[0], Long.parseLong(data[1]), Long
							.parseLong(data[2])));
		}

	}

	public static class SortByPartReducer extends
			Reducer<Text, FlowBean, Text, FlowBean> {

		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,Context context)
				throws IOException, InterruptedException {
		
			long up_flow_count = 0;
			long down_flow_count = 0;
			for (FlowBean value : values) {
				up_flow_count += value.getUp_flow();
				down_flow_count += value.getDown_flow();
			}
			context.write(key, new FlowBean(key.toString(),up_flow_count,down_flow_count));
			
		}
		
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SortByPart.class);
		job.setMapperClass(SortByPartMapper.class);
		job.setReducerClass(SortByPartReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		job.setPartitionerClass(AreaPartitioner.class);
		
		job.setNumReduceTasks(7);
		
		FileInputFormat.setInputPaths(job, new Path("/home/muxiaocao/myFile/srcdata/"));
		FileOutputFormat.setOutputPath(job, new Path("/home/muxiaocao/myFile/output3"));
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SortByPart(), args);
		System.out.println(exitCode);
	}
}
