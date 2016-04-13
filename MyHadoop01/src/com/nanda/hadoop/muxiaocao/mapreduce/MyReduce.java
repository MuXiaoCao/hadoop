package com.nanda.hadoop.muxiaocao.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 框架在map处理完成之后，将所有key、value对缓存起来，进行分组，
 * 然后传递一个组<key,value{}>,调用一次reduce方法
 * @author muxiaocao
 *
 */
public class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {
		long count = 0;
		// 便利value，进行累加计数
		for (LongWritable value : values) {
			count += value.get();
		}
		// 输出单词key的统计结果
		context.write(key, new LongWritable(count));
	}

}
