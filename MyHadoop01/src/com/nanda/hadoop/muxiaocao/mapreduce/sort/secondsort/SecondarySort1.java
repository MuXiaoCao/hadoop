package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util.RecordParser;

/**
 * 对所有数据进行分析，淘汰后两周没有数据的
 * @author muxiaocao
 *
 */
public class SecondarySort1 extends Configured implements
		Tool {

	static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, TextPair, TextPair> {
		long count = 0;
		RecordParser parser = RecordParser.getInstance(TextPair.class);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			/*if (count > 5000000) {
				return;
			}
			count++;*/

			if (value.toString().contains("user_id")) {
				return;
			}
				
			TextPair dataInstance = (TextPair) parser.parse(value, ",");
			if (dataInstance != null) {
				if (dataInstance.getTime().get() > new LongWritable(120500).get()) {
					context.write(dataInstance, dataInstance);
				}
			}

		}

	}

	static class MaxTemperatureReducer extends
			Reducer<TextPair, TextPair, Text, Text> {

		private StringBuffer sb;

		@Override
		protected void reduce(TextPair key, Iterable<TextPair> values,
				Context context) throws IOException, InterruptedException {
			sb = new StringBuffer();
			for (TextPair textPair : values) {
				sb.append(textPair.toString());
			}
			context.write(key.getUserID(), new Text(sb.toString()));
		}
	}

	public static class FirstPartitioner extends
			Partitioner<TextPair, NullWritable> {

		@Override
		public int getPartition(TextPair key, NullWritable value,
				int numPartitions) {
			return Math.abs(key.getUserID().hashCode() % 10000 * 127)
					% numPartitions;
		}
	}

	public static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(TextPair.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TextPair textPair1 = (TextPair) a;
			TextPair textPair2 = (TextPair) b;
			int cmp = textPair1.compareTo(textPair2);
			if (cmp != 0) {
				return cmp;
			}
			return textPair1.getTime().compareTo(textPair2.getTime());
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return WritableComparator.compareBytes(b1, s1, l1,
					b2, s2, l2);
		}
		
	}

	public static class GroupComparator implements RawComparator<TextPair> {

		public int compare(TextPair a, TextPair b) {
			TextPair textPair1 = (TextPair) a;
			TextPair textPair2 = (TextPair) b;
			int cmp = textPair1.compareTo(textPair2);
			if (cmp != 0) {
				return cmp;
			}
			return textPair1.getTime().compareTo(textPair2.getTime());
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8,
					b2, s2, Integer.SIZE / 8);
		}

		/*
		 * public int compare(TextPair o1, TextPair o2) { int first1 =
		 * o1.getUserID().hashCode(); int first2 = o2.getUserID().hashCode();
		 * return first1 - first2; }
		 * 
		 * public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int
		 * l2) { return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8,
		 * b2, s2, Integer.SIZE/8); }
		 */

		/*
		 * protected GroupComparator() { super(TextPair.class,true); }
		 * 
		 * @Override public int compare(WritableComparable a, WritableComparable
		 * b) { TextPair textPair1 = (TextPair)a; TextPair textPair2 =
		 * (TextPair)b;
		 * 
		 * return textPair1.compareTo(textPair2); }
		 */

		/*
		 * @Override public int compare(byte[] b1, int s1, int l1, byte[] b2,
		 * int s2, int l2) { return WritableComparator.compareBytes(b1, s1,
		 * Integer.SIZE/8, b2, s2, Integer.SIZE/8); }
		 * 
		 * @Override public int compare(TextPair o1, TextPair o2) { int first1 =
		 * o1.getUserID().hashCode(); int first2 = o2.getUserID().hashCode();
		 * return first1 - first2; }
		 */

	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new SecondarySort1(), new String[] {
						"hdfs://muxiaocao:9000/myFile/userdata.csv",
						"/home/muxiaocao/sfd/user3" });
		System.out.println(exitCode);
	}
}
