package com.nanda.hadoop.muxiaocao.mapreduce.sort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortMR extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(SortMR.class);

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class SortMapper extends
			Mapper<LongWritable, Text, FlowBean, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = StringUtils.split(line, "\t");
			System.out.println(data[0] + data[1] + data[2]);
			context.write(
					new FlowBean(data[0], Long.parseLong(data[1]), Long
							.parseLong(data[2])), NullWritable.get());
		}

	}

	public static class SortReducer extends
			Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new SortMR(),
				new String[] { "/home/muxiaocao/myFile/srcdata/sort.txt",
						"/home/muxiaocao/myFile/output2" });
		System.out.println(res);
		System.exit(res);

	}
}
