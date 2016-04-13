package com.nanda.hadoop.muxiaocao.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 4个泛型，强两个是制定mapper输入数据的类型，
 * KEYIN是输入key的类型，VALUEIN是输入的value的类型
 * map和reduce的数据输入输出都是一key-value对的形式封装的
 * 默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中一行的起始偏移量，value是这一行的内容
 * @author muxiaocao
 *
 */
public class MyMap extends Mapper<LongWritable, Text, Text, LongWritable>{

	/**
	 * mapreduce框架每读一行数据就会调用map方法
	 * 具体业务逻辑就写在这个方法体中，而且我们要处理的业务数据，已经被框架以方法参数的形式传输
	 * 其中，key是这一行数据的起始偏移量
	 *           value是这一行的文本内容
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] words = StringUtils.split(line, " ");
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1L));
		}
	}
}
