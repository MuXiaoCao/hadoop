package com.nanda.hadoop.muxiaocao.mapreduce.bs;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ItemDeal21 extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ItemDeal21.class);

		job.setMapperClass(ItemMapper.class);
		job.setReducerClass(ItemReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(DefinedPartition.class); //设置自定义分区策略 
          
	        job.setGroupingComparatorClass(DefinedGroupSort.class); //设置自定义分组策略 
	        //job.setSortComparatorClass(DefinedComparator.class); //设置自定义二次排序策略
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ItemBeanSimple.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ItemMapper extends
			Mapper<LongWritable, Text, Text, ItemBeanSimple> {

		/*private String line;
		private String[] datas;
		private String[] items;
		private String[] items2;*/
		private Text userID = new Text();
		private LongWritable time = new LongWritable();
		private Text itemID = new Text();
		private IntWritable behavior = new IntWritable();
		private ItemBeanSimple  iBeanSimple = new ItemBeanSimple();
		
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
				iBeanSimple = new ItemBeanSimple();
				//userID.set(data[len-2]);
				userID.set(data[0].trim());
				time.set(Long.parseLong(data[len-1].replaceAll("-", "").replaceAll(" ", "").replaceAll("	", "").trim()));
				itemID.set(data[1].trim());
				//System.out.println(itemID.toString());
				behavior.set(Integer.parseInt(data[2].trim()));
				iBeanSimple.setBehavior(behavior);
				iBeanSimple.setItemID(itemID.toString());
				iBeanSimple.setTime(time);
				iBeanSimple.setUserID(userID);
				//String time, String itemID, int behavior, String category
				context.write( userID,iBeanSimple);
			}
		}

	}

	public static long lineCount = 0;
	public static long count = 0;

	public static class ItemReducer extends
			Reducer<Text, ItemBeanSimple, Text, Text> {

		TreeSet<ItemBeanSimple> tree = new TreeSet<ItemBeanSimple>(new mycomp());
		StringBuffer sb = new StringBuffer();

		@Override
		protected void reduce(Text key, Iterable<ItemBeanSimple> values,
				Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (ItemBeanSimple itemBean : values) {
				sb.append(itemBean);
			}
			context.write(key, new Text(sb.toString()));
		}
		class mycomp implements Comparator<ItemBeanSimple> {

			/* (non-Javadoc)
			 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
			 */
			public int compare(ItemBeanSimple itemBean1, ItemBeanSimple itemBean2) {
				System.out.println("-------------");
				System.out.println(itemBean1.getTime());
				System.out.println(itemBean2.getTime());
				System.out.println(itemBean1.hashCode());
				System.out.println(itemBean2.hashCode());
				int a = (int)(itemBean1.getTime().get() - itemBean2.getTime().get())%1000000*1000000 + (itemBean1.hashCode() - itemBean2.hashCode())%100000;
				System.out.println(a);
				return 1;
			}

		 }  

	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new ItemDeal21(),
				new String[] { "hdfs://muxiaocao:9000/myFile/userdata.csv",
						"/home/muxiaocao/sfd/output2" });
		System.out.println(res);
		System.exit(res);

	}
	
	
}
