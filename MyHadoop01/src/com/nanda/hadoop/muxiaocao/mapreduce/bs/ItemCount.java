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
import org.apache.hadoop.mapreduce.lib.join.ArrayListBackedIterator;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ItemCount extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ItemCount.class);

		job.setMapperClass(ItemMapper.class);
		job.setReducerClass(ItemReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ItemMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = StringUtils.split(line, ",");
			int len = data.length; 
			if (!data[0].equals("user_id")) {
				context.write(new Text(data[1]), new LongWritable(1L));
				/*context.write(new Text(data[0]), new ItemBean(data[len-1], data[1],
						Integer.parseInt(data[2]), data[len-2]));*/
			}
		}

	}

	public static long lineCount = 0;
	public static long count = 0;

	public static class ItemReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		private ArrayListBackedIterator<ItemBean> userItem = new ArrayListBackedIterator<ItemBean>() {

			@Override
			public String toString() {
				StringBuffer sb = new StringBuffer();
				ItemBean itemBean = null;
				try {
					while (this.hasNext()) {
						this.next(itemBean);
						sb.append(itemBean + "-");
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				return sb.toString();
			}

		};

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long count = 0;
			System.out.println(userItem);
			for (LongWritable value : values) {
				count += value.get();
			}
			context.write(key, new LongWritable(count));
		}

	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new ItemCount(),
				new String[] { "hdfs://sfd:9000/userdata.csv",
						"/home/sfd/muxiaocao/output" });
		System.out.println(res);
		System.exit(res);

	}
}
