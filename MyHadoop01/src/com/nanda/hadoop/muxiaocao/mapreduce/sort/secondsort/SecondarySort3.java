package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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

import com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util.RecordParser;

/**
 * 根据用户的历史数据，统计用户的购买记录和未购买记录
 * 
 * @author muxiaocao
 *
 */
public class SecondarySort3 extends Configured implements Tool {
	
	static long count = 0;
	/**
	 * 输出textkey2(userID+itemID) 和 text 购买记录
	 * @author muxiaocao
	 *
	 */
	// 所有数据
	static class MaxTemperatureMapper2 extends Mapper<LongWritable, Text, Text, Text> {
		RecordParser parser = RecordParser.getInstance(TextPair.class);
		TextPair2 pair2;
		String[] datas;
		TextKey2 resultKey = new TextKey2();
		Text tmp;
		ItemBean resultValue = new ItemBean();
		// itemID 和 数据矩阵
		TreeMap<Text, Boolean> history = new TreeMap<Text, Boolean>();
		// itemID
		String newKey;
		//1205起第几天 
		int days;
		/**
		 * 数据矩阵
		 */
		int[][] historyD;
		/**
		 * 指标权重
		 */
		double[] Param = new double[]{0.6,0.1,0.15,0.15};
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			count++;

			if (value.toString().contains("user_id")) {
				return;
			}
			TextPair dataInstance = (TextPair) parser.parse(value, ",");
			resultKey.set(dataInstance.getUserID(), dataInstance.getItemID());
			if (dataInstance.getBehave().get() == 4) {
				if (resultKey.getUserID().toString().equals("492")) {
					System.out.println(resultKey.getUserID() + "-" + resultKey.getItemID() + " map :" + dataInstance.toString());
				}
				context.write(resultKey.getUserID(), resultKey.getItemID());
			}
		}
	}
	static class MaxTemperatureReducer2 extends Reducer<Text, Text, Text, Text> {

		private StringBuffer sb;
		private Text resultKey = new Text();
		private Text resultValue = new Text();
		HashMap<String,Integer> map;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		
			sb = new StringBuffer();
			map = new HashMap<String, Integer>();
			boolean flag = key.toString().equals("492") ;
			
			for (Text itemBean : values) {
				if (flag) {
					System.out.println(itemBean.toString());
				}
				if (map.containsKey(itemBean.toString())) {
					map.replace(itemBean.toString(), map.get(itemBean.toString())+1);
					if (flag) {
						System.out.println("旧的" + itemBean.toString());
					}
				}else {
					if (flag) {
						System.out.println("新的" + itemBean.toString());
					}
					map.put(itemBean.toString(), 1);
				}
			}
			
			Set<String> keySet = map.keySet();
			for (String text : keySet) {
				//sb.append(text + "," + map.get(text)+";");
				context.write(key, new Text(text + "\t" + map.get(text)));
			}
		}
	}
	
	
	
	/*============================*/
	// 后14天有数据的
	static class MaxTemperatureMapper extends Mapper<LongWritable, Text, TextKey2, Text> {
		RecordParser parser = RecordParser.getInstance(TextPair2.class);
		TextPair2 pair2;
		String[] datas;
		TextKey2 resultKey = new TextKey2();
		Text tmp;
		ItemBean resultValue = new ItemBean();
		// itemID 和 数据矩阵
		TreeMap<Text, Boolean> history = new TreeMap<Text, Boolean>();
		// itemID
		String newKey;
		//1205起第几天 
		int days;
		/**
		 * 数据矩阵
		 */
		int[][] historyD;
		/**
		 * 指标权重
		 */
		double[] Param = new double[]{0.6,0.1,0.15,0.15};
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			datas = StringUtils.split(value.toString(), "\t");
			count++;
			resultKey.setUserID(new Text(datas[0]));
		
			datas = StringUtils.split(datas[1], ";");
			// 构造用户历史记录矩阵
			for (int i = 0; i < datas.length; i++) {

				pair2 = (TextPair2) parser.parse(datas[i], ",");
				newKey = pair2.getItemID().toString();
				tmp = new Text(newKey);
				if (!history.containsKey(tmp)) {
					history.put(tmp, false);
				}
				if (pair2.getBehave().get() == 4) {
					history.put(tmp,true);
				}
			}
			Set<Text> keySet = history.keySet();
			for (Text text : keySet) {
				if (history.get(text)) {
					resultKey.setItemID(text);
					context.write(resultKey, text);
				}
			}
			history.clear();
		}
		
		/**
		 * 从用户历史记录中得出用户对某个商品的购买情况
		 * @param historyData
		 * @return
		 */
		public boolean isShop(int[][] historyData) {
			int x = 0;
			for (int i = 0; i < historyData[0].length; i++) {
				x += historyData[3][i];
			}
			return x > 0 ? true : false ;
		}
	}
	
	static class MaxTemperatureReducer extends Reducer<TextKey2, Text, Text, Text> {

		private StringBuffer sb;
		private Text resultKey = new Text();
		private Text resultValue = new Text();
		
		@Override
		protected void reduce(TextKey2 key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println("this is reduce:" + key.toString());
			sb = new StringBuffer();
			for (Text itemBean : values) {
				sb.append(itemBean.toString() + ",");
			}
			resultValue.set((sb.toString()));
			context.write(key.getUserID(),resultValue);
			System.out.println(count);
		}
	}

	public static class FirstPartitioner extends Partitioner<TextKey2, Text> {

		@Override
		public int getPartition(TextKey2 key, Text value, int numPartitions) {
			return Math.abs(key.getUserID().hashCode() % 10000 * 127) % numPartitions;
		}
	}

	public static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(TextKey2.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TextKey2 textKeya = (TextKey2) a;
			TextKey2 textKeyb= (TextKey2) b;
			int cmp = textKeya.getUserID().compareTo(textKeyb.getUserID());
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
			TextKey2 textKeyb= (TextKey2) b;
			int cmp = textKeya.getUserID().compareTo(textKeyb.getUserID());
			if (cmp != 0) {
				return cmp;
			}
			return textKeya.getItemID().compareTo(textKeyb.getItemID());
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8, b2, s2, Integer.SIZE / 8);
		}


	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//job.setMapperClass(MaxTemperatureMapper.class);
		//job.setReducerClass(MaxTemperatureReducer.class);
		job.setMapperClass(MaxTemperatureMapper2.class);
		job.setReducerClass(MaxTemperatureReducer2.class);
		//job.setPartitionerClass(FirstPartitioner.class);
		//job.setSortComparatorClass(KeyComparator.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//job.setMapOutputKeyClass(TextKey2.class);
		//job.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		/*int exitCode = ToolRunner.run(new Configuration(), new SecondarySort3(),
				new String[] { "/home/muxiaocao/sfd/user3/part-r-00000", "/home/muxiaocao/sfd/user5" });*/
		int exitCode = ToolRunner.run(new Configuration(), new SecondarySort3(),
				new String[] { "hdfs://muxiaocao:9000/myFile/userdata.csv", "/home/muxiaocao/sfd/user6/newuser63/" });
		System.out.println(exitCode);
	}
	
}
