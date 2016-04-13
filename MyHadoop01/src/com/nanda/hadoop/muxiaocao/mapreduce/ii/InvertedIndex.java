package com.nanda.hadoop.muxiaocao.mapreduce.ii;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MR实现倒序索引
 * @author muxiaocao
 *
 */
public class InvertedIndex extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(InvertedIndex.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PositionBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * map类
	 * 负责读取文本信息，分割出单词，并生成对应的PositionBean，记录文件名和数量
	 * 按单词分类，提交给reducer
	 * @author muxiaocao
	 *
	 */
	public static class InvertedIndexMapper extends
			Mapper<LongWritable, Text, Text, PositionBean> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName();
			String line = value.toString();
			String[] split = StringUtils.split(line, " ");
			for (String word : split) {
				//System.out.println(word);
				PositionBean bean = new PositionBean(fileName,word,1L);
				context.write(new Text(word), bean);
			}
		}
	}
	/**
	 * reducer类
	 * 负责将对应单词的所有bean进行整合（去重，count累加计数）。
	 * 注意：重写toString方法
	 * @author muxiaocao
	 *
	 */
	public static class InvertedIndexReducer extends
			Reducer<Text, PositionBean, Text, MapWritable> {

		MapWritable mapWritable = new MapWritable(){
			
			@Override
			public String toString() {
				StringBuffer sb = new StringBuffer();
				for (Map.Entry<Writable, Writable> e: this.entrySet()) {
					sb.append(((Text)e.getKey()).toString()).append("--").append(((LongWritable)e.getValue()).get() + " ");
				}
				return sb.toString();
			}
			
		};
		@Override
		protected void reduce(Text key, Iterable<PositionBean> values,
				Context context) throws IOException, InterruptedException {
			for (PositionBean value : values) {
				Text valueText = new Text(value.getFileName());
				if (mapWritable.containsKey(valueText)) {
					LongWritable count = (LongWritable) mapWritable.get(valueText);
					count.set(count.get() + value.getCount());
					mapWritable.put(valueText, count);
				} else {
					mapWritable.put(valueText, new LongWritable(value.getCount()));
				}
			}
			context.write(key, mapWritable);
			mapWritable.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new InvertedIndex(),
				new String[] { "/home/muxiaocao/myFile/srcdata/files/",
						"/home/muxiaocao/myFile/output4/" });
		System.exit(res);
	}
}
