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

public class ItemDeal2 extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ItemDeal2.class);

		job.setMapperClass(ItemMapper.class);
		job.setReducerClass(ItemReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
			
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ItemBeanSimple.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ItemMapper extends
			Mapper<LongWritable, Text, Text, ItemBeanSimple> {

		private Text userID = new Text();
		private LongWritable time = new LongWritable();
		private Text itemID = new Text();
		private IntWritable behavior = new IntWritable();
		private ItemBeanSimple  iBeanSimple = new ItemBeanSimple();
		//LongWritable time, Text itemID, IntWritable behavior,Text userID
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			//System.out.println(line);
			String[] data = StringUtils.split(line, "\t");
			String[] items = StringUtils.split(data[1],";");
			//System.out.println("items:" + data[1]);
			for (int i = 0; i < items.length; i++) {
				//System.out.println("item" + i + ":" + items[i]);
				String[] item = StringUtils.split(items[i],",");
				iBeanSimple = new ItemBeanSimple();
				userID.set(data[0]);
				time.set(Long.parseLong(item[0].trim()));
				itemID.set(item[1].trim());
				behavior.set(Integer.parseInt(item[2].trim()));
				iBeanSimple.setBehavior(behavior);
				iBeanSimple.setItemID(itemID.toString());
				iBeanSimple.setTime(time);
				iBeanSimple.setUserID(userID);
				context.write( userID,iBeanSimple);
			}
		}

	}

	public static long lineCount = 0;
	public static long count = 0;

	public static class ItemReducer extends
			Reducer<Text, ItemBeanSimple, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<ItemBeanSimple> values,
				Context context) throws IOException, InterruptedException {
			TreeSet<ItemBeanSimple> tree = new TreeSet<ItemBeanSimple>(new mycomp());
			StringBuffer sb = new StringBuffer();
			count = 0;
			for (ItemBeanSimple value : values) {
				tree.add(value);
				System.out.println(value.toString());
				count++;
			}
			//System.out.println(count);
			Iterator<ItemBeanSimple> iterator = tree.iterator();

		    while (iterator.hasNext()) {
		    		sb.append(iterator.next());
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

		int res = ToolRunner.run(new Configuration(), new ItemDeal2(),
				new String[] { "hdfs://muxiaocao:9000/myFile/user.txt",
						"/home/muxiaocao/sfd/output1" });
		System.out.println(res);
		System.exit(res);

	}
	
	
}
