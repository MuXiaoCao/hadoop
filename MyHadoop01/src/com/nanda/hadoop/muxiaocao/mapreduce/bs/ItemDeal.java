package com.nanda.hadoop.muxiaocao.mapreduce.bs;

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

public class ItemDeal extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ItemDeal.class);

		job.setMapperClass(ItemMapper.class);
		job.setReducerClass(ItemReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ItemBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ItemMapper extends
			Mapper<LongWritable, Text, Text, ItemBean> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
		
			if (lineCount > 1000000) {
				return ;
			}
			String[] data = StringUtils.split(line, ",");
			int len = data.length;
			lineCount += 1L;
			if (!data[0].equals("user_id")) {
				context.write(new Text(data[0]), new ItemBean(data[len-1], data[1],
						Integer.parseInt(data[2]), data[len-2]));
			}
		}

	}

	public static long lineCount = 0;
	public static long count = 0;

	public static class ItemReducer extends
			Reducer<Text, ItemBean, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<ItemBean> values,
				Context context) throws IOException, InterruptedException {
			
				StringBuffer sb = new StringBuffer();
				for (ItemBean itemBean : values) {
					sb.append(itemBean);
				}
				context.write(key, new Text(sb.toString()));
		}

	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new ItemDeal(),
				new String[] { "hdfs://sfd:9000/userdata.csv",
						"/home/sfd/muxiaocao/user1.txt" });
		System.out.println(res);
		System.exit(res);

	}
}
