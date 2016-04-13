package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.junit.Test;

import com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util.RecordParser;

/**
 * 根据用户的历史数据，统计用户对商品的近期关注度
 * 
 * @author muxiaocao
 * 
 */
public class SecondarySort2 extends Configured implements Tool {

	/**
	 * 输出textkey2(userID+itemID) 和 itembean（itemID + 关注度）
	 * 
	 * @author muxiaocao
	 * 
	 */
	static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, TextKey2, ItemBean> {
		long count = 0;
		RecordParser parser = RecordParser.getInstance(TextPair2.class);
		TextPair2 pair2;
		String[] datas;
		TextKey2 resultKey = new TextKey2();
		Text tmp;
		ItemBean resultValue = new ItemBean();
		// itemID 和 数据矩阵
		TreeMap<Text, int[][]> history = new TreeMap<Text, int[][]>();
		// itemID
		String newKey;
		// 1205起第几天
		int days;
		/**
		 * 数据矩阵
		 */
		int[][] historyD;
		/**
		 * 指标权重
		 */
		double[] Param = new double[] { 0.6, 0.1, 0.15, 0.15 };

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			datas = StringUtils.split(value.toString(), "\t");

			resultKey.setUserID(new Text(datas[0]));

			datas = StringUtils.split(datas[1], ";");
			// 构造用户历史记录矩阵
			for (int i = 0; i < datas.length; i++) {

				pair2 = (TextPair2) parser.parse(datas[i], ",");
				newKey = pair2.getItemID().toString();
				tmp = new Text(newKey);
				if (!history.containsKey(tmp)) {
					history.put(tmp, new int[4][14]);
				}
				days = (int) (pair2.getTime().get() - 120500L) / 100;
				historyD = history.get(tmp);
				/*
				 * System.out.println(pair2.toString());
				 * System.out.println(days);
				 */
				historyD[pair2.getBehave().get() - 1][days]++;
				history.put(tmp, historyD);
			}
			Set<Text> keySet = history.keySet();
			for (Text text : keySet) {
				historyD = history.get(text);
				resultValue.setCount(new IntWritable(
						(int) (getCount(historyD) * 100)));
				resultValue.setItemID(text);
				resultKey.setItemID(text);
				// System.out.println("this is 2 :" + resultKey.toString() + " "
				// + resultValue.toString());
				if (resultValue.getCount().get() > 0) {
					context.write(resultKey, resultValue);
				}
			}
			history.clear();
		}


		/**
		 * 从用户历史记录中得出用户对某个商品的关注度
		 * 
		 * @param historyData
		 * @return
		 */
		public double getCount(int[][] historyData) {

			double sum = 0;
			int p1 = historyData[3][0] + historyData[3][1];
			int p2 = historyData[3][7] + historyData[3][8];
			int p3 = historyData[3][13] * historyData[3][12]
					* historyData[3][11];
			boolean flag = p1 * p2 > 0 ? true : false;
			if (flag || p3 > 0) {
				return 1.0;// 周期购买或者连续购买的用户
			} else {
				p1 = historyData[2][13] + historyData[2][12];
				if (p1 > 0) {
					sum += Param[0];// 近两天加入购物车的用户
				}
				for (int i = 0; i < 11; i++) {
					if (historyData[2][i] > 0) {
						flag = true;
						break;
					}
				}
				// 判断是否加入过购物车
				if (flag) {
					for (int i = 0; i < 14; i++) {
						if (historyData[2][i] > 0) {
							sum += Param[2];// 加入购物车且收藏过的用户
							break;
						}
					}
				} else {
					p1 = historyData[0][13] * historyData[0][12]
							* historyData[0][11];
					if (p1 > 0) {
						sum += Param[3];// 最近三天都在关注的用户
					}
				}
			}
			return sum;
		}
	}

	static class MaxTemperatureReducer extends
			Reducer<TextKey2, ItemBean, Text, NullWritable> {

		private StringBuffer sb;
		private Text resultKey = new Text();
		private Text resultValue = new Text();

		@Override
		protected void reduce(TextKey2 key, Iterable<ItemBean> values,
				Context context) throws IOException, InterruptedException {
			// System.out.println("this is reduce:" + key.toString());
			/*sb = new StringBuffer();
			for (ItemBean itemBean : values) {
				sb.append(itemBean);
			}
			resultValue.set((sb.toString()));
			context.write(key.getUserID(), resultValue);*/

			for (ItemBean itemBean : values) {
				context.write(new Text(key.getUserID().toString() + "," + itemBean.getItemID()),NullWritable.get());
			}
		}
	}

	public static class FirstPartitioner extends
			Partitioner<TextKey2, ItemBean> {

		@Override
		public int getPartition(TextKey2 key, ItemBean value, int numPartitions) {
			return Math.abs(key.getUserID().hashCode() % 10000 * 127)
					% numPartitions;
		}
	}

	public static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(TextKey2.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TextKey2 textKeya = (TextKey2) a;
			TextKey2 textKeyb = (TextKey2) b;
			int cmp = textKeya.compareTo(textKeyb);
			if (cmp != 0) {
				return cmp;
			}
			
			return textKeya.getItemID().compareTo(textKeyb.getItemID());
		}
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
		}

	}

	public static class GroupComparator implements RawComparator<TextKey2> {

		public int compare(TextKey2 a, TextKey2 b) {
			TextKey2 textKeya = (TextKey2) a;
			TextKey2 textKeyb = (TextKey2) b;
			int cmp = textKeya.compareTo(textKeyb);
			if (cmp != 0) {
				return cmp;
			}
			return textKeya.getItemID().compareTo(textKeyb.getItemID());
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8,
					b2, s2, Integer.SIZE / 8);
		}

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
		job.setOutputValueClass(NullWritable.class);

		job.setMapOutputKeyClass(TextKey2.class);
		job.setMapOutputValueClass(ItemBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(),
				new SecondarySort2(), new String[] {
						"/home/muxiaocao/sfd/user3/part-r-00000",
						"/home/muxiaocao/sfd/user4/tijiao1csv" });
		System.out.println(exitCode);

		/*
		 * int[][] data = new int[][] { { 1, 2, 3, 4, 5, 6, 8, 0, 0, 1, 0, 1, 1,
		 * 1 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }, { 0, 0, 0, 0, 0,
		 * 0, 0, 0, 0, 0, 0, 0, 1, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		 * 0 } }; System.out.println(getCount(data));
		 */
	}

	public static double getCount(int[][] historyData) {
		double[] Param = new double[] { 0.6, 0.1, 0.15, 0.15 };

		double sum = 0;
		int p1 = historyData[3][0] + historyData[3][1];
		int p2 = historyData[3][7] + historyData[3][8];
		int p3 = historyData[3][13] * historyData[3][12] * historyData[3][11];
		boolean flag = p1 * p2 > 0 ? true : false;
		if (flag || p3 > 0) {
			return 1.0;// 周期购买或者连续购买的用户
		} else {
			p1 = historyData[2][13] + historyData[2][12];

			if (p1 > 0) {
				sum += Param[0];// 近两天加入购物车的用户
			}
			for (int i = 0; i < 11; i++) {
				if (historyData[2][i] > 0) {
					flag = true;
					break;
				}
			}
			// 判断是否加入过购物车
			if (flag) {
				for (int i = 0; i < 14; i++) {
					if (historyData[2][i] > 0) {
						sum += Param[2];// 加入购物车且收藏过的用户
						break;
					}
				}
			} else {
				p1 = historyData[0][13] * historyData[0][12]
						* historyData[0][11];
				if (p1 > 0) {
					sum += Param[3];// 最近三天都在关注的用户
				}
			}
		}
		return sum;
	}
}
